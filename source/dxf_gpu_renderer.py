"""Vulkan-preferred DXF and cable graph renderer for CableRouteResolver.

The public class intentionally keeps the QGraphicsView-like methods used by the
existing editor while moving the main viewport onto Qt Quick's RHI scene graph.
Qt Quick is requested to use Vulkan when a Vulkan loader is available.  The
renderer falls back to OpenGL (or the platform-selected RHI backend) when Vulkan
is unavailable or has been disabled with ``CABLE_ROUTER_RENDER_BACKEND``.

Performance design:
* event-driven rendering with a fixed maximum frame rate;
* independent retained DXF, edge, object and overlay textures;
* dirty-layer invalidation instead of unconditional full redraws;
* coalesced pointer-drag events;
* viewport and level-of-detail culling;
* tiled, immutable DXF geometry caches;
* QStaticText label caching;
* fast interaction mode with a delayed final-quality frame.
"""

from __future__ import annotations

import ctypes
import math
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from PySide6.QtCore import (
    QPoint,
    QPointF,
    QRect,
    QRectF,
    QSize,
    Qt,
    QTimer,
    QUrl,
    Signal,
)
from PySide6.QtGui import (
    QBrush,
    QColor,
    QFont,
    QPainter,
    QPainterPath,
    QPen,
    QPolygonF,
    QStaticText,
    QTransform,
)
from PySide6.QtOpenGLWidgets import QOpenGLWidget
from PySide6.QtWidgets import QRubberBand

# QVulkanInstance is not exposed by every PySide6 build, even when Qt Quick
# can still use the Vulkan RHI backend.  Import it opportunistically so an
# older/minimal binding never prevents the application from starting.
try:  # pragma: no cover - availability depends on the installed PySide6 wheel.
    from PySide6.QtGui import QVulkanInstance
except (ImportError, AttributeError):  # PySide6 build without the wrapper.
    QVulkanInstance = None

try:  # Qt Quick is the Vulkan/RHI path.
    from PySide6.QtQml import QQmlComponent
    from PySide6.QtQuick import (
        QQuickItem,
        QQuickPaintedItem,
        QQuickWindow,
        QSGRendererInterface,
    )
    from PySide6.QtQuickWidgets import QQuickWidget

    _QT_QUICK_AVAILABLE = True
except Exception:  # pragma: no cover - depends on the installed PySide6 build.
    QQmlComponent = None
    QQuickItem = None
    QQuickPaintedItem = object
    QQuickWindow = None
    QSGRendererInterface = None
    QQuickWidget = None
    _QT_QUICK_AVAILABLE = False

from network_schema import network_instances_for_floor

Bounds = Tuple[float, float, float, float]
PointTuple = Tuple[float, float]


def _enum_member(owner: Any, enum_name: str, member: str) -> Any:
    nested = getattr(owner, enum_name, None)
    if nested is not None and hasattr(nested, member):
        return getattr(nested, member)
    return getattr(owner, member, None)


def _vulkan_loader_available() -> bool:
    """Cheap loader probe performed before the first Qt Quick window exists."""
    if os.environ.get("CABLE_ROUTER_DISABLE_VULKAN", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return False
    candidates: Sequence[str]
    if sys.platform.startswith("win"):
        candidates = ("vulkan-1.dll",)
    elif sys.platform == "darwin":
        # MoltenVK may be supplied by the application or SDK.
        candidates = ("libMoltenVK.dylib", "libvulkan.1.dylib", "libvulkan.dylib")
    else:
        candidates = ("libvulkan.so.1", "libvulkan.so")
    for name in candidates:
        try:
            ctypes.CDLL(name)
            return True
        except OSError:
            continue
    return False


def _probe_vulkan_runtime() -> bool:
    """Return whether Vulkan should be requested for Qt Quick.

    Some PySide6 wheels omit ``QVulkanInstance`` while retaining Vulkan support
    in Qt Quick/RHI.  In that case the platform Vulkan loader is the best safe
    pre-window probe available; actual device creation remains Qt Quick's job.
    """
    loader_available = _vulkan_loader_available()
    if not loader_available:
        return False

    if QVulkanInstance is None:
        return True

    try:
        instance = QVulkanInstance()
        created = bool(instance.create())
        if created:
            try:
                instance.destroy()
            except Exception:
                pass
        return created
    except Exception:
        return False


_BACKEND_CONFIGURED = False
_REQUESTED_BACKEND = "unconfigured"


def _configure_qt_quick_backend() -> str:
    """Select the RHI API immediately before the first QQuickWidget exists."""
    global _BACKEND_CONFIGURED, _REQUESTED_BACKEND
    if _BACKEND_CONFIGURED:
        return _REQUESTED_BACKEND

    requested = os.environ.get("CABLE_ROUTER_RENDER_BACKEND", "vulkan").strip().lower()
    if requested in {"", "auto"}:
        requested = "vulkan" if _probe_vulkan_runtime() else "opengl"
    elif requested == "vulkan" and not _probe_vulkan_runtime():
        requested = "opengl"

    if not _QT_QUICK_AVAILABLE:
        _REQUESTED_BACKEND = "opengl-widget"
        _BACKEND_CONFIGURED = True
        return _REQUESTED_BACKEND

    api_names = {
        "vulkan": "Vulkan",
        "opengl": "OpenGL",
        "gl": "OpenGL",
        "d3d11": "Direct3D11",
        "direct3d11": "Direct3D11",
        "d3d12": "Direct3D12",
        "direct3d12": "Direct3D12",
        "metal": "Metal",
        "software": "Software",
    }
    member = api_names.get(requested, "Vulkan")
    api = _enum_member(QSGRendererInterface, "GraphicsApi", member)
    if api is None:
        _REQUESTED_BACKEND = "qt-rhi-auto"
    else:
        try:
            QQuickWindow.setGraphicsApi(api)
            os.environ["QSG_RHI_BACKEND"] = requested
            _REQUESTED_BACKEND = requested
        except Exception:
            _REQUESTED_BACKEND = "qt-rhi-auto"
    _BACKEND_CONFIGURED = True
    return _REQUESTED_BACKEND

_ViewBase = QQuickWidget if _QT_QUICK_AVAILABLE else QOpenGLWidget


class DirtyLayer:
    DXF = 1 << 0
    EDGES = 1 << 1
    OBJECTS = 1 << 2
    OVERLAY = 1 << 3
    VIEW = DXF | EDGES | OBJECTS
    ALL = VIEW | OVERLAY


@dataclass
class _DxfTile:
    line_path: QPainterPath = field(default_factory=QPainterPath)
    poly_path: QPainterPath = field(default_factory=QPainterPath)
    arc_path: QPainterPath = field(default_factory=QPainterPath)
    text_entities: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DxfRenderCache:
    """Immutable tiled DXF paths shared by all viewport frames."""

    source_key: Optional[Tuple[Any, ...]] = None
    origin_x: float = 0.0
    origin_y: float = 0.0
    tile_size: float = 25.0
    tiles: Dict[Tuple[int, int], _DxfTile] = field(default_factory=dict)
    global_tile: _DxfTile = field(default_factory=_DxfTile)

    def clear(self) -> None:
        self.source_key = None
        self.origin_x = 0.0
        self.origin_y = 0.0
        self.tile_size = 25.0
        self.tiles.clear()
        self.global_tile = _DxfTile()


@dataclass
class _FrameSnapshot:
    floor: int
    all_points: Dict[str, dict]
    floor_points: Dict[str, dict]
    departments: Dict[str, dict]
    data: Dict[str, Any]
    network_instances: Dict[str, dict]
    all_network_instances: Dict[str, dict]
    network_assets: Dict[str, dict]


class _PointerEventSnapshot:
    """Small safe event proxy used when mouse-move events are coalesced."""

    def __init__(self, event: Any):
        self._position = QPointF(event.position())
        try:
            self._global_position = QPointF(event.globalPosition())
        except Exception:
            self._global_position = QPointF(self._position)
        try:
            self._buttons = event.buttons()
        except Exception:
            self._buttons = Qt.LeftButton
        try:
            self._button = event.button()
        except Exception:
            self._button = Qt.NoButton
        try:
            self._modifiers = event.modifiers()
        except Exception:
            self._modifiers = Qt.NoModifier

    def position(self) -> QPointF:
        return QPointF(self._position)

    def globalPosition(self) -> QPointF:
        return QPointF(self._global_position)

    def buttons(self):
        return self._buttons

    def button(self):
        return self._button

    def modifiers(self):
        return self._modifiers


if _QT_QUICK_AVAILABLE:

    class _PaintLayer(QQuickPaintedItem):
        def __init__(self, owner: "GpuDxfGraphView", layer_mask: int, parent_item: Any):
            super().__init__(parent_item)
            self.owner = owner
            self.layer_mask = int(layer_mask)
            self.setParentItem(parent_item)
            self.setAntialiasing(False)
            try:
                self.setFillColor(
                    owner._background if layer_mask == DirtyLayer.DXF else QColor(0, 0, 0, 0)
                )
            except Exception:
                pass
            if layer_mask == DirtyLayer.DXF:
                try:
                    self.setOpaquePainting(True)
                except Exception:
                    pass
            try:
                self.setAcceptedMouseButtons(Qt.NoButton)
            except Exception:
                pass

        def paint(self, painter: QPainter) -> None:  # noqa: N802 - Qt virtual name
            self.owner._paint_layer(painter, self.layer_mask)


class GpuDxfGraphView(_ViewBase):
    """Vulkan-preferred retained viewport with an OpenGL compatibility fallback."""

    leftClicked = Signal(object, float, float)
    leftDoubleClicked = Signal(object, float, float)
    leftReleased = Signal(object)
    rightClicked = Signal(object, float, float)
    middleClicked = Signal(object)
    middleDragged = Signal(object)
    middleReleased = Signal(object)
    mouseWheelScrolled = Signal(object)
    mouseDragged = Signal(object, float, float)
    mouseMoved = Signal(object, float, float)

    def __init__(self, parent=None):
        _configure_qt_quick_backend()
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setAutoFillBackground(False)

        self.store = None
        self._store_revision = None
        self.dxf_scene = None
        self.floor = 0

        self.show_dxf = True
        self.show_labels = True
        self.show_graph = True
        self.show_overlay = True
        self.show_edges = True
        self.show_nodes = True
        self.show_data_points = True
        self.show_unconnected_data_points_only = False
        self.unconnected_data_point_names: set[str] = set()
        self.show_routing_unconnected_data_points_only = False
        self.routing_unconnected_data_point_names: set[str] = set()
        self.hide_connected_data_points = False
        self.connected_data_point_names: set[str] = set()
        self.show_locations = True
        self.show_comms_rooms = True
        self.show_placement_zones = True
        self.show_departments = True
        self.show_network = True
        self.show_network_links = True
        self.show_network_connections = True
        self.show_network_assets = True
        self.show_wireless_devices = True
        self.show_physical_fibre = True

        self.selected_point_name: Optional[str] = None
        self.selected_template_names: set[str] = set()
        self.edge_chain_start: Optional[str] = None
        self.selected_placement_zone_id: Optional[str] = None
        self.placement_zone_preview: Optional[Dict[str, Any]] = None
        self.equipment_room_extent_overlay: Optional[Dict[str, Any]] = None
        self.data_room_measurement_overlay: Optional[Dict[str, Any]] = None

        self._scale = 1.0
        self._offset = QPointF(0.0, 0.0)
        # The last transform baked into the retained painted-layer textures.
        # During pan/zoom the Qt Quick scene graph transforms those textures
        # directly on the GPU; geometry is repainted only after interaction
        # settles or model data changes.
        self._committed_scale = 1.0
        self._committed_offset = QPointF(0.0, 0.0)
        self._live_view_transform_active = False
        self._last_middle_pos: Optional[QPoint] = None
        self._overlay_provider = None
        self._dxf_cache = DxfRenderCache()
        self._bound_dxf_key: Optional[Tuple[Any, ...]] = None
        self._frame_snapshot: Optional[_FrameSnapshot] = None
        self._rubber_band = QRubberBand(QRubberBand.Rectangle, self)
        self._static_text_cache: Dict[Tuple[str, int, bool], QStaticText] = {}
        # Graph and DXF labels use cached vector glyph outlines rather than
        # scaled QStaticText bitmaps. This keeps settled text sharp at every
        # camera scale without deriving font size from zoom.
        self._vector_text_cache: Dict[Tuple[str, bool], Tuple[QPainterPath, QRectF]] = {}

        # Retained graph textures use 1920 x 1080 as the minimum working
        # viewport, but only a modest overscan margin is cached around it.
        # The previous one-full-viewport margin produced 5760 x 3240 textures
        # at Full HD and consumed excessive memory bandwidth during zoom.
        self._cache_base_width_px = max(
            1920.0,
            float(os.environ.get("CABLE_ROUTER_CACHE_BASE_WIDTH", "1920") or 1920),
        )
        self._cache_base_height_px = max(
            1080.0,
            float(os.environ.get("CABLE_ROUTER_CACHE_BASE_HEIGHT", "1080") or 1080),
        )
        self._cache_margin_factor = max(
            0.15,
            min(
                1.0,
                float(
                    os.environ.get("CABLE_ROUTER_CACHE_MARGIN_FACTOR", "0.35")
                    or 0.35
                ),
            ),
        )
        self._cache_margin_max_x_px = max(
            256.0,
            float(
                os.environ.get("CABLE_ROUTER_CACHE_MARGIN_MAX_X_PX", "1280")
                or 1280
            ),
        )
        self._cache_margin_max_y_px = max(
            192.0,
            float(
                os.environ.get("CABLE_ROUTER_CACHE_MARGIN_MAX_Y_PX", "720")
                or 720
            ),
        )
        self._cache_margin_x = min(
            self._cache_margin_max_x_px,
            self._cache_base_width_px * self._cache_margin_factor,
        )
        self._cache_margin_y = min(
            self._cache_margin_max_y_px,
            self._cache_base_height_px * self._cache_margin_factor,
        )

        # QQuickPaintedItem normally allocates one texture per retained layer at
        # the item size multiplied by the display device-pixel ratio. A large
        # maximised HiDPI window can therefore request several very large
        # textures at once. Keep every layer inside a configurable physical
        # texture budget and reduce overscan before reducing visible-resolution.
        self._max_layer_texture_side_px = max(
            1024,
            int(
                os.environ.get(
                    "CABLE_ROUTER_MAX_LAYER_TEXTURE_SIDE_PX", "4096"
                )
                or 4096
            ),
        )
        self._max_layer_texture_pixels = max(
            1_048_576,
            int(
                os.environ.get(
                    "CABLE_ROUTER_MAX_LAYER_TEXTURE_PIXELS", "6291456"
                )
                or 6_291_456
            ),
        )
        self._resize_settle_ms = max(
            30,
            int(
                os.environ.get("CABLE_ROUTER_RESIZE_SETTLE_MS", "90")
                or 90
            ),
        )
        self._pan_rebake_pending = False

        # Label dimensions are fixed in model space.  These constants never
        # depend on camera zoom; the camera transform alone changes the
        # apparent screen size.  Defaults are intentionally slightly larger
        # than the previous 0.25-unit graph labels.
        self._graph_label_world_height = max(
            0.05,
            float(os.environ.get("CABLE_ROUTER_GRAPH_LABEL_WORLD_HEIGHT", "0.25") or 0.25),
        )
        self._dxf_label_world_scale = max(
            0.25,
            float(os.environ.get("CABLE_ROUTER_DXF_LABEL_WORLD_SCALE", "0.6") or 0.6),
        )
        self._zoom_settle_ms = max(
            120,
            int(os.environ.get("CABLE_ROUTER_ZOOM_SETTLE_MS", "280") or 280),
        )

        self._background = QColor("#111111")
        self._dxf_line_pen = QPen(QColor("#858585"), 0.0)
        self._dxf_poly_pen = QPen(QColor("#bebebe"), 0.0)
        self._dxf_arc_pen = QPen(QColor("#2e2e2e"), 0.0)
        self._same_floor_edge_pen = QPen(QColor("#6aa9ff"), 0.0)
        self._cross_floor_edge_pen = QPen(QColor("#ff4d4f"), 0.0)

        self._target_fps = max(
            5, min(120, int(os.environ.get("CABLE_ROUTER_TARGET_FPS", "30") or 30))
        )
        self._frame_interval_ms = max(8, int(round(1000.0 / self._target_fps)))
        self._dirty_layers = DirtyLayer.ALL
        self._frame_timer = QTimer(self)
        self._frame_timer.setTimerType(Qt.PreciseTimer)
        self._frame_timer.setInterval(self._frame_interval_ms)
        self._frame_timer.timeout.connect(self._flush_redraw)

        self._quality_timer = QTimer(self)
        self._quality_timer.setSingleShot(True)
        self._quality_timer.timeout.connect(self._end_fast_interaction)
        self._fast_interaction = False
        self._interaction_finish_layers = 0

        # Maximise/full-screen transitions can emit a burst of resize events.
        # Reallocating four retained layer textures for every intermediate size
        # can exhaust graphics memory. Rebuild the textures once resizing settles.
        self._resize_settle_timer = QTimer(self)
        self._resize_settle_timer.setSingleShot(True)
        self._resize_settle_timer.setInterval(self._resize_settle_ms)
        self._resize_settle_timer.timeout.connect(self._finish_deferred_resize)
        self._screen_change_connected = False

        self._drag_timer = QTimer(self)
        self._drag_timer.setSingleShot(True)
        self._drag_timer.setTimerType(Qt.PreciseTimer)
        self._drag_timer.timeout.connect(self._emit_pending_drag)
        self._pending_drag: Optional[Tuple[_PointerEventSnapshot, float, float]] = None
        self._last_drag_emit = 0.0

        self._frame_counter = 0
        self._fps_window_start = time.monotonic()
        self._actual_fps = 0.0
        self._last_frame_duration_ms = 0.0
        self._max_graph_labels = max(
            100, int(os.environ.get("CABLE_ROUTER_MAX_GRAPH_LABELS", "2500") or 2500)
        )
        self._max_dxf_labels = max(
            50, int(os.environ.get("CABLE_ROUTER_MAX_DXF_LABELS", "1500") or 1500)
        )

        self._quick_mode = bool(_QT_QUICK_AVAILABLE)
        self._layer_items: Dict[int, Any] = {}
        self._quick_component = None
        self._quick_root = None
        if self._quick_mode:
            self._initialise_quick_scene()
        self.request_redraw(DirtyLayer.ALL)

    # ------------------------------------------------------------------
    # Qt Quick / frame scheduling
    # ------------------------------------------------------------------
    def _initialise_quick_scene(self) -> None:
        try:
            self.setClearColor(self._background)
            resize_enum = getattr(QQuickWidget, "ResizeMode", QQuickWidget)
            resize_mode = getattr(resize_enum, "SizeRootObjectToView", None)
            if resize_mode is not None:
                self.setResizeMode(resize_mode)
            component = QQmlComponent(self.engine())
            component.setData(b"import QtQuick 2.15\nItem { width: 1; height: 1 }", QUrl())
            root = component.create()
            if root is None:
                raise RuntimeError("Could not create the Qt Quick renderer root item")
            self.setContent(QUrl(), component, root)
            self._quick_component = component
            self._quick_root = root
            for z, mask in enumerate(
                (DirtyLayer.DXF, DirtyLayer.EDGES, DirtyLayer.OBJECTS, DirtyLayer.OVERLAY)
            ):
                item = _PaintLayer(self, mask, root)
                item.setZ(float(z))
                # Retained textures are temporarily scaled/translated during
                # navigation. Smooth sampling and mipmaps avoid the blocky
                # appearance that would otherwise occur before the final
                # high-quality repaint.
                try:
                    item.setSmooth(True)
                except Exception:
                    pass
                # Mipmap generation is expensive for large painted textures.
                # Keep it disabled by default; smooth filtering is sufficient
                # during the short live zoom transform and avoids a costly
                # mip-chain rebuild after every settled zoom.
                if str(os.environ.get("CABLE_ROUTER_TEXTURE_MIPMAP", "0")).strip().lower() in {
                    "1", "true", "yes", "on"
                }:
                    try:
                        item.setMipmap(True)
                    except Exception:
                        pass
                # Top-left origin makes the retained camera transform exact:
                # screen' = ratio * screen + delta. It also avoids centre
                # compensation changing the apparent label scale.
                try:
                    origin = _enum_member(QQuickItem, "TransformOrigin", "TopLeft")
                    if origin is not None:
                        item.setTransformOrigin(origin)
                except Exception:
                    pass
                self._layer_items[mask] = item
            self._resize_quick_layers()
            try:
                quick_window = self.quickWindow()
                quick_window.setPersistentSceneGraph(True)
                quick_window.setPersistentGraphics(True)
            except Exception:
                pass
        except Exception as exc:
            raise RuntimeError(
                "Qt Quick RHI could not initialise the main graph viewer. "
                "Set CABLE_ROUTER_RENDER_BACKEND=opengl to force the compatibility "
                "backend, or update the graphics driver."
            ) from exc

    def _device_pixel_ratio(self) -> float:
        try:
            value = float(self.devicePixelRatioF())
        except (AttributeError, TypeError, ValueError):
            value = 1.0
        if not math.isfinite(value) or value <= 0.0:
            return 1.0
        return value

    def _texture_scale_for_size(self, width: float, height: float) -> float:
        """Return a uniform logical-size scale that stays in the GPU budget."""
        width = max(1.0, float(width))
        height = max(1.0, float(height))
        dpr = self._device_pixel_ratio()
        physical_width = width * dpr
        physical_height = height * dpr
        physical_pixels = physical_width * physical_height

        scale = 1.0
        if physical_width > self._max_layer_texture_side_px:
            scale = min(scale, self._max_layer_texture_side_px / physical_width)
        if physical_height > self._max_layer_texture_side_px:
            scale = min(scale, self._max_layer_texture_side_px / physical_height)
        if physical_pixels > self._max_layer_texture_pixels:
            scale = min(
                scale,
                math.sqrt(self._max_layer_texture_pixels / physical_pixels),
            )
        return max(0.001, min(1.0, scale))

    def _bounded_texture_size(self, width: float, height: float) -> QSize:
        # QQuickPaintedItem multiplies textureSize by the window DPR. Supply a
        # reduced logical texture size when the requested physical texture would
        # exceed the configured side or total-pixel limit. Painting coordinates
        # remain in item space, so no drawing code needs to change.
        scale = self._texture_scale_for_size(width, height)
        return QSize(
            max(1, int(round(max(1.0, float(width)) * scale))),
            max(1, int(round(max(1.0, float(height)) * scale))),
        )

    def _bounded_cache_margins(
        self,
        width: float,
        height: float,
        margin_x: float,
        margin_y: float,
    ) -> Tuple[float, float]:
        """Reduce overscan first so the visible viewport stays at full detail."""
        margin_x = max(0.0, float(margin_x))
        margin_y = max(0.0, float(margin_y))
        if not margin_x and not margin_y:
            return 0.0, 0.0

        # If the visible viewport alone exceeds the budget, margins cannot help;
        # _bounded_texture_size() will safely lower the retained resolution.
        if self._texture_scale_for_size(width, height) < 0.999:
            return 0.0, 0.0

        requested_width = width + margin_x * 2.0
        requested_height = height + margin_y * 2.0
        if self._texture_scale_for_size(requested_width, requested_height) >= 0.999:
            return margin_x, margin_y

        low = 0.0
        high = 1.0
        for _ in range(18):
            factor = (low + high) / 2.0
            candidate_width = width + margin_x * factor * 2.0
            candidate_height = height + margin_y * factor * 2.0
            if self._texture_scale_for_size(candidate_width, candidate_height) >= 0.999:
                low = factor
            else:
                high = factor
        return margin_x * low, margin_y * low

    def _resize_quick_layers(self) -> None:
        if not self._quick_mode:
            return
        width = max(1.0, float(self.width()))
        height = max(1.0, float(self.height()))
        base_width = max(width, self._cache_base_width_px)
        base_height = max(height, self._cache_base_height_px)
        requested_margin_x = min(
            self._cache_margin_max_x_px,
            base_width * self._cache_margin_factor,
        )
        requested_margin_y = min(
            self._cache_margin_max_y_px,
            base_height * self._cache_margin_factor,
        )
        self._cache_margin_x, self._cache_margin_y = self._bounded_cache_margins(
            width,
            height,
            requested_margin_x,
            requested_margin_y,
        )
        if self._quick_root is not None:
            try:
                self._quick_root.setWidth(width)
                self._quick_root.setHeight(height)
            except Exception:
                pass
        for mask, item in self._layer_items.items():
            if mask == DirtyLayer.OVERLAY:
                item_width = width
                item_height = height
                item_x = 0.0
                item_y = 0.0
            else:
                item_width = width + (self._cache_margin_x * 2.0)
                item_height = height + (self._cache_margin_y * 2.0)
                item_x = -self._cache_margin_x
                item_y = -self._cache_margin_y

            # Set the bounded backing texture before changing the item size so
            # the scene graph never briefly creates the unrestricted allocation.
            try:
                item.setTextureSize(
                    self._bounded_texture_size(item_width, item_height)
                )
            except (AttributeError, RuntimeError):
                pass
            item.setX(item_x)
            item.setY(item_y)
            item.setWidth(item_width)
            item.setHeight(item_height)

    def _finish_deferred_resize(self) -> None:
        self._resize_quick_layers()
        self.request_redraw(DirtyLayer.ALL)

    def _screen_changed(self, *_args) -> None:
        # Recalculate the physical texture budget when moved between displays
        # with different DPI scaling.
        self._resize_settle_timer.start(self._resize_settle_ms)

    def request_redraw(self, layers: int = DirtyLayer.ALL) -> None:
        self._dirty_layers |= int(layers)
        if not self.isVisible():
            return
        if not self._frame_timer.isActive():
            self._frame_timer.start()

    def update(self, *args, **kwargs) -> None:  # QGraphicsView compatibility
        self.request_redraw(DirtyLayer.ALL)

    def _schedule_scene_graph_update(self) -> None:
        if not self._quick_mode:
            self.request_redraw(DirtyLayer.VIEW)
            return
        try:
            self.quickWindow().update()
        except Exception:
            # Setting QQuickItem properties normally schedules a scene-graph
            # frame itself. This is only a compatibility fallback.
            pass

    def _apply_live_view_transform(self) -> None:
        """Move retained view textures without repainting their geometry."""
        if not self._quick_mode:
            self.request_redraw(DirtyLayer.VIEW)
            return
        base_scale = max(1.0e-9, float(self._committed_scale))
        ratio = float(self._scale) / base_scale
        delta_x = float(self._offset.x()) - (ratio * float(self._committed_offset.x()))
        delta_y = float(self._offset.y()) - (ratio * float(self._committed_offset.y()))
        # View-layer local coordinates include the overscan margin. With a
        # top-left transform origin the following produces the exact desired
        # camera transform while keeping the larger cached area around it.
        item_x = delta_x - (ratio * self._cache_margin_x)
        item_y = delta_y - (ratio * self._cache_margin_y)
        for mask in (DirtyLayer.DXF, DirtyLayer.EDGES, DirtyLayer.OBJECTS):
            item = self._layer_items.get(mask)
            if item is None:
                continue
            item.setScale(ratio)
            item.setX(item_x)
            item.setY(item_y)
        self._live_view_transform_active = True
        self._schedule_scene_graph_update()
        # The retained texture covers the viewport, so very large camera moves
        # can expose uncached margins. Re-bake only after a substantial move or
        # zoom, not for every pointer event.
        pan_budget_x = max(180.0, self._cache_margin_x * 0.76)
        pan_budget_y = max(140.0, self._cache_margin_y * 0.76)
        needs_rebake = (
            abs(delta_x) > pan_budget_x
            or abs(delta_y) > pan_budget_y
            or ratio < 0.50
            or ratio > 2.00
        )
        if needs_rebake:
            if self._last_middle_pos is not None:
                # Never reset a live middle-pan transform while the button is
                # held. Re-bake after release only if the smaller overscan has
                # genuinely been consumed.
                self._pan_rebake_pending = True
            elif not self._fast_interaction:
                # Wheel zoom remains a pure scene-graph transform while input
                # is active. The interaction-settle timer performs one final
                # geometry bake instead of repainting between wheel notches.
                self.request_redraw(DirtyLayer.VIEW)

    def _commit_live_view_transform(self) -> None:
        """Reset scene-graph transforms before baking the current view."""
        if self._quick_mode:
            for mask in (DirtyLayer.DXF, DirtyLayer.EDGES, DirtyLayer.OBJECTS):
                item = self._layer_items.get(mask)
                if item is None:
                    continue
                item.setScale(1.0)
                item.setX(-self._cache_margin_x)
                item.setY(-self._cache_margin_y)
        self._committed_scale = float(self._scale)
        self._committed_offset = QPointF(self._offset)
        self._live_view_transform_active = False

    def pan_by(self, dx: float, dy: float) -> None:
        """Pan retained layers without scheduling a release-time re-render.

        The scene-graph transform is already the final camera transform.  A
        delayed repaint used to reset the retained items before their new
        textures were ready, which caused a visible jump when panning ended.
        Geometry is re-baked only when a real model/layer change requires it.
        """
        self._offset += QPointF(float(dx), float(dy))
        self._apply_live_view_transform()

    def _flush_redraw(self) -> None:
        if not self.isVisible():
            self._frame_timer.stop()
            return
        if self._quick_mode and self._resize_settle_timer.isActive():
            # Keep dirty flags intact; the settle callback will resize once and
            # then schedule the final repaint.
            self._frame_timer.stop()
            return
        layers = self._dirty_layers
        self._dirty_layers = 0
        if not layers:
            self._frame_timer.stop()
            return

        started = time.perf_counter()
        # A layer repaint must use one coherent view transform. If retained
        # textures are currently being moved by the scene graph, commit the
        # final camera and repaint all view layers together exactly once.
        if layers & DirtyLayer.VIEW and self._live_view_transform_active:
            layers |= DirtyLayer.VIEW
            self._commit_live_view_transform()
        elif layers & DirtyLayer.VIEW:
            self._committed_scale = float(self._scale)
            self._committed_offset = QPointF(self._offset)

        if layers & (DirtyLayer.EDGES | DirtyLayer.OBJECTS):
            self._ensure_frame_snapshot()

        if self._quick_mode:
            for mask, item in self._layer_items.items():
                if layers & mask:
                    item.update()
        else:  # pragma: no cover - only used by PySide builds without Qt Quick.
            super().update()

        self._last_frame_duration_ms = (time.perf_counter() - started) * 1000.0
        self._frame_counter += 1
        now = time.monotonic()
        elapsed = now - self._fps_window_start
        if elapsed >= 1.0:
            self._actual_fps = self._frame_counter / elapsed
            self._frame_counter = 0
            self._fps_window_start = now

        if not self._dirty_layers:
            self._frame_timer.stop()

    def _mark_interaction(
        self,
        final_layers: int = DirtyLayer.VIEW,
        settle_ms: int = 120,
    ) -> None:
        self._fast_interaction = True
        self._interaction_finish_layers |= int(final_layers)
        self._quality_timer.start(max(60, int(settle_ms)))

    def _end_fast_interaction(self) -> None:
        if not self._fast_interaction:
            return
        self._fast_interaction = False
        layers = self._interaction_finish_layers
        self._interaction_finish_layers = 0
        if layers:
            self.request_redraw(layers)

    def set_target_fps(self, fps: int) -> None:
        self._target_fps = max(5, min(120, int(fps)))
        self._frame_interval_ms = max(8, int(round(1000.0 / self._target_fps)))
        self._frame_timer.setInterval(self._frame_interval_ms)

    def target_fps(self) -> int:
        return int(self._target_fps)

    def renderer_backend_name(self) -> str:
        if not self._quick_mode:
            return "OpenGL widget fallback"
        try:
            api = self.quickWindow().rendererInterface().graphicsApi()
            text = str(api).split(".")[-1]
            if text and text != "Unknown":
                return f"Qt Quick RHI ({text})"
        except Exception:
            pass
        return f"Qt Quick RHI ({_REQUESTED_BACKEND})"

    def render_stats(self) -> Dict[str, Any]:
        return {
            "backend": self.renderer_backend_name(),
            "target_fps": self._target_fps,
            "actual_fps": round(self._actual_fps, 1),
            "last_frame_ms": round(self._last_frame_duration_ms, 2),
            "idle_when_clean": True,
            "fast_interaction": bool(self._fast_interaction),
        }

    # ------------------------------------------------------------------
    # Public API used by app.py and network_integration.py
    # ------------------------------------------------------------------
    def set_store(self, store: Any, revision: Any = None) -> None:
        if self.store is store and self._store_revision == revision:
            return
        self.store = store
        self._store_revision = revision
        self.invalidate_store_cache()

    def invalidate_store_cache(self) -> None:
        self._frame_snapshot = None
        self.request_redraw(DirtyLayer.EDGES | DirtyLayer.OBJECTS)

    notify_store_changed = invalidate_store_cache

    def invalidate_moving_object_cache(self) -> None:
        """Refresh movable graph objects during a drag without redrawing edges.

        Large projects can contain thousands of routing/cable edges.  Repainting
        that edge layer for every mouse-move makes dragged items appear frozen
        until the button is released.  During the live drag the endpoint marker
        positions are the important visual feedback, so only the object layer is
        invalidated.  The editor performs one full edge/object refresh on
        release so connected routes end in the correct final position.
        """
        self._frame_snapshot = None
        self.request_redraw(DirtyLayer.OBJECTS)

    notify_moving_object_changed = invalidate_moving_object_cache

    def _dxf_source_key(self, scene: Any) -> Tuple[Any, ...]:
        entities = getattr(scene, "entities", None) if scene is not None else None
        return (
            getattr(scene, "path", None) if scene is not None else None,
            getattr(scene, "revision", None) if scene is not None else None,
            id(entities),
            len(entities or []),
        )

    def set_dxf_scene(self, dxf_scene: Any) -> None:
        new_key = self._dxf_source_key(dxf_scene)
        self.dxf_scene = dxf_scene
        if self._bound_dxf_key != new_key:
            self._bound_dxf_key = new_key
            self._dxf_cache.clear()
            self.request_redraw(DirtyLayer.DXF | DirtyLayer.OVERLAY)

    def set_floor(self, floor: int) -> None:
        floor = int(floor)
        if floor == self.floor:
            return
        self.floor = floor
        self._frame_snapshot = None
        self.request_redraw(DirtyLayer.ALL)

    def set_visible_layers(
        self,
        *,
        show_dxf: Optional[bool] = None,
        show_labels: Optional[bool] = None,
        show_graph: Optional[bool] = None,
        show_overlay: Optional[bool] = None,
        show_edges: Optional[bool] = None,
        show_nodes: Optional[bool] = None,
        show_data_points: Optional[bool] = None,
        show_unconnected_data_points_only: Optional[bool] = None,
        unconnected_data_point_names: Optional[Sequence[str]] = None,
        show_routing_unconnected_data_points_only: Optional[bool] = None,
        routing_unconnected_data_point_names: Optional[Sequence[str]] = None,
        hide_connected_data_points: Optional[bool] = None,
        connected_data_point_names: Optional[Sequence[str]] = None,
        show_locations: Optional[bool] = None,
        show_comms_rooms: Optional[bool] = None,
        show_placement_zones: Optional[bool] = None,
        show_departments: Optional[bool] = None,
        show_network: Optional[bool] = None,
        show_network_links: Optional[bool] = None,
        show_network_connections: Optional[bool] = None,
        show_network_assets: Optional[bool] = None,
        show_wireless_devices: Optional[bool] = None,
        show_physical_fibre: Optional[bool] = None,
        **_future_layers: Any,
    ) -> None:
        changes = 0

        def assign(attr: str, value: Optional[bool], dirty: int) -> None:
            nonlocal changes
            if value is None:
                return
            new_value = bool(value)
            if bool(getattr(self, attr)) != new_value:
                setattr(self, attr, new_value)
                changes |= dirty

        assign("show_dxf", show_dxf, DirtyLayer.DXF)
        assign("show_labels", show_labels, DirtyLayer.DXF | DirtyLayer.OBJECTS)
        assign("show_graph", show_graph, DirtyLayer.EDGES | DirtyLayer.OBJECTS)
        assign("show_overlay", show_overlay, DirtyLayer.OVERLAY)
        assign("show_edges", show_edges, DirtyLayer.EDGES)
        assign("show_nodes", show_nodes, DirtyLayer.OBJECTS)
        assign("show_data_points", show_data_points, DirtyLayer.OBJECTS)
        assign(
            "show_unconnected_data_points_only",
            show_unconnected_data_points_only,
            DirtyLayer.OBJECTS,
        )
        assign(
            "show_routing_unconnected_data_points_only",
            show_routing_unconnected_data_points_only,
            DirtyLayer.OBJECTS,
        )
        assign(
            "hide_connected_data_points",
            hide_connected_data_points,
            DirtyLayer.OBJECTS,
        )

        def assign_names(attr: str, values: Optional[Sequence[str]]) -> None:
            nonlocal changes
            if values is None:
                return
            new_value = {str(value).strip() for value in values if str(value).strip()}
            if getattr(self, attr) != new_value:
                setattr(self, attr, new_value)
                changes |= DirtyLayer.OBJECTS

        assign_names("unconnected_data_point_names", unconnected_data_point_names)
        assign_names(
            "routing_unconnected_data_point_names",
            routing_unconnected_data_point_names,
        )
        assign_names("connected_data_point_names", connected_data_point_names)
        assign("show_locations", show_locations, DirtyLayer.OBJECTS)
        assign("show_comms_rooms", show_comms_rooms, DirtyLayer.OBJECTS)
        assign("show_placement_zones", show_placement_zones, DirtyLayer.OBJECTS)
        assign("show_departments", show_departments, DirtyLayer.OBJECTS)
        assign("show_network", show_network, DirtyLayer.EDGES | DirtyLayer.OBJECTS)
        link_value = show_network_connections
        if link_value is None:
            link_value = show_network_links
        assign("show_network_links", link_value, DirtyLayer.EDGES)
        self.show_network_connections = self.show_network_links
        assign("show_network_assets", show_network_assets, DirtyLayer.OBJECTS)
        assign("show_wireless_devices", show_wireless_devices, DirtyLayer.OBJECTS)
        assign("show_physical_fibre", show_physical_fibre, DirtyLayer.EDGES | DirtyLayer.OBJECTS)
        if changes:
            self.request_redraw(changes)

    def set_selection(
        self,
        selected_point_name: Optional[str],
        selected_template_names: Optional[Iterable[str]] = None,
        edge_chain_start: Optional[str] = None,
    ) -> None:
        names = set(selected_template_names or [])
        if (
            selected_point_name == self.selected_point_name
            and names == self.selected_template_names
            and edge_chain_start == self.edge_chain_start
        ):
            return
        self.selected_point_name = selected_point_name
        self.selected_template_names = names
        self.edge_chain_start = edge_chain_start
        self.request_redraw(DirtyLayer.OBJECTS | DirtyLayer.OVERLAY)

    def set_placement_zone_selection(self, zone_id: Optional[str]) -> None:
        zone_id = str(zone_id or "").strip() or None
        if zone_id == self.selected_placement_zone_id:
            return
        self.selected_placement_zone_id = zone_id
        self.request_redraw(DirtyLayer.OBJECTS | DirtyLayer.OVERLAY)

    def set_placement_zone_preview(self, zone: Optional[Dict[str, Any]]) -> None:
        preview = dict(zone) if isinstance(zone, dict) else None
        if preview == self.placement_zone_preview:
            return
        self.placement_zone_preview = preview
        self.request_redraw(DirtyLayer.OBJECTS)

    def set_equipment_room_extent_overlay(
        self, overlay: Optional[Dict[str, Any]]
    ) -> None:
        value = dict(overlay) if isinstance(overlay, dict) else None
        if value == self.equipment_room_extent_overlay:
            return
        self.equipment_room_extent_overlay = value
        self.request_redraw(DirtyLayer.OBJECTS)

    def set_data_room_measurement_overlay(
        self, overlay: Optional[Dict[str, Any]]
    ) -> None:
        value = dict(overlay) if isinstance(overlay, dict) else None
        if value == self.data_room_measurement_overlay:
            return
        self.data_room_measurement_overlay = value
        self.request_redraw(DirtyLayer.OBJECTS)

    def set_overlay_provider(self, overlay_provider) -> None:
        if overlay_provider is self._overlay_provider:
            return
        self._overlay_provider = overlay_provider
        self.request_redraw(DirtyLayer.OVERLAY)

    def invalidate_dxf_cache(self, force: bool = False) -> None:
        # The host historically called this after many graph-only operations.
        # DXF content is revision-keyed by set_dxf_scene(), so ignore redundant
        # invalidations unless a caller explicitly requests a forced rebuild.
        current_key = self._dxf_source_key(self.dxf_scene)
        if not force and current_key == self._bound_dxf_key:
            return
        self._bound_dxf_key = current_key
        self._dxf_cache.clear()
        self.request_redraw(DirtyLayer.DXF)

    def invalidate_overlay(self) -> None:
        self.request_redraw(DirtyLayer.OVERLAY)

    def transform(self) -> QTransform:
        return QTransform().scale(self._scale, self._scale)

    def viewport(self):
        return self

    def mapToScene(self, value):  # noqa: N802 - QGraphicsView compatibility
        if isinstance(value, QRect):
            rect = value.normalized()
            return QPolygonF(
                [
                    self.screen_to_scene(rect.topLeft()),
                    self.screen_to_scene(rect.topRight()),
                    self.screen_to_scene(rect.bottomRight()),
                    self.screen_to_scene(rect.bottomLeft()),
                ]
            )
        return self.screen_to_scene(value)

    def centerOn(self, value, y: Optional[float] = None) -> None:  # noqa: N802
        if y is None:
            scene_pos = QPointF(value)
        else:
            scene_pos = QPointF(float(value), float(y))
        centre = QPointF(self.width() / 2.0, self.height() / 2.0)
        self._offset = centre - QPointF(scene_pos.x() * self._scale, scene_pos.y() * self._scale)
        self._mark_interaction(DirtyLayer.VIEW)
        self._apply_live_view_transform()

    def scale(self, sx: float, sy: float) -> None:
        factor = float(sx)
        centre = QPointF(self.width() / 2.0, self.height() / 2.0)
        before = self.screen_to_world(centre)
        self._scale = max(0.001, min(5000.0, self._scale * factor))
        after_screen = self.world_to_screen(before[0], before[1])
        self._offset += centre - after_screen
        self._mark_interaction(DirtyLayer.VIEW)
        self._apply_live_view_transform()

    def resetTransform(self) -> None:  # noqa: N802
        self._scale = 1.0
        self._offset = QPointF(0.0, 0.0)
        self._commit_live_view_transform()
        self.request_redraw(DirtyLayer.VIEW)

    def fitInView(
        self, rect: QRectF, aspect_mode: Qt.AspectRatioMode = Qt.KeepAspectRatio
    ) -> None:  # noqa: N802
        self.fit_to_rect(rect, aspect_mode)

    def fit_to_content(self, padding: float = 8.0) -> None:
        rect = self.content_scene_rect(padding=padding)
        if rect is not None:
            self.fit_to_rect(rect, Qt.KeepAspectRatio)

    def fit_to_rect(
        self, rect: QRectF, aspect_mode: Qt.AspectRatioMode = Qt.KeepAspectRatio
    ) -> None:
        if rect.isNull() or rect.width() <= 0 or rect.height() <= 0:
            return
        margin = 30.0
        available_w = max(1.0, self.width() - margin * 2.0)
        available_h = max(1.0, self.height() - margin * 2.0)
        sx = available_w / rect.width()
        sy = available_h / rect.height()
        self._scale = min(sx, sy) if aspect_mode == Qt.KeepAspectRatio else max(sx, sy)
        self._scale = max(0.001, min(5000.0, self._scale))
        centre_scene = rect.center()
        centre_screen = QPointF(self.width() / 2.0, self.height() / 2.0)
        self._offset = centre_screen - QPointF(
            centre_scene.x() * self._scale, centre_scene.y() * self._scale
        )
        self._commit_live_view_transform()
        self.request_redraw(DirtyLayer.VIEW)

    def content_scene_rect(self, padding: float = 8.0) -> Optional[QRectF]:
        bounds = self._content_bounds()
        if bounds is None:
            return None
        min_x, min_y, max_x, max_y = bounds
        return QRectF(
            min_x - padding,
            -(max_y + padding),
            max(1.0, (max_x - min_x) + padding * 2.0),
            max(1.0, (max_y - min_y) + padding * 2.0),
        )

    def world_to_scene(self, x: float, y: float) -> QPointF:
        return QPointF(float(x), -float(y))

    def scene_to_world(self, sx: float, sy: float) -> Tuple[float, float]:
        return float(sx), -float(sy)

    def world_to_screen(self, x: float, y: float) -> QPointF:
        scene = self.world_to_scene(x, y)
        return QPointF(
            scene.x() * self._scale + self._offset.x(),
            scene.y() * self._scale + self._offset.y(),
        )

    def screen_to_scene(self, pos: QPointF | QPoint) -> QPointF:
        return QPointF(
            (float(pos.x()) - self._offset.x()) / self._scale,
            (float(pos.y()) - self._offset.y()) / self._scale,
        )

    def screen_to_world(self, pos: QPointF | QPoint) -> Tuple[float, float]:
        scene = self.screen_to_scene(pos)
        return self.scene_to_world(scene.x(), scene.y())

    def visible_world_bounds(self, padding_px: float = 60.0) -> Bounds:
        # Geometry is painted into an overscanned retained texture, not merely
        # the visible widget. Include that full cache area in all culling tests
        # so panning reveals already-rendered DXF, routes, objects and labels.
        left = -(self._cache_margin_x + padding_px)
        top = -(self._cache_margin_y + padding_px)
        right = self.width() + self._cache_margin_x + padding_px
        bottom = self.height() + self._cache_margin_y + padding_px
        p1 = self.screen_to_world(QPointF(left, top))
        p2 = self.screen_to_world(QPointF(right, bottom))
        return min(p1[0], p2[0]), min(p1[1], p2[1]), max(p1[0], p2[0]), max(p1[1], p2[1])

    def _cached_screen_rect(self, padding_px: float = 0.0) -> QRectF:
        """Screen-space extent currently baked into retained view textures."""
        return QRectF(
            -self._cache_margin_x - padding_px,
            -self._cache_margin_y - padding_px,
            float(self.width())
            + (self._cache_margin_x * 2.0)
            + (padding_px * 2.0),
            float(self.height())
            + (self._cache_margin_y * 2.0)
            + (padding_px * 2.0),
        )

    def find_nearest_selectable_name(
        self, x: float, y: float, radius_px: float = 12.0
    ) -> Optional[str]:
        if self.store is None:
            return None
        radius_world = max(0.2, radius_px / max(self._scale, 0.001))
        best_name = None
        best_dist = radius_world
        snapshot = self._ensure_frame_snapshot()
        for name, point in snapshot.floor_points.items():
            kind = str(point.get("kind", "")).strip()
            if kind == "corridor_node" and not self.show_nodes:
                continue
            if kind == "data_point":
                if not self.show_data_points:
                    continue
                if (
                    self.show_unconnected_data_points_only
                    and str(name) not in self.unconnected_data_point_names
                ):
                    continue
                if (
                    self.show_routing_unconnected_data_points_only
                    and str(name) not in self.routing_unconnected_data_point_names
                ):
                    continue
                if (
                    self.hide_connected_data_points
                    and str(name) in self.connected_data_point_names
                ):
                    continue
            if kind == "comms_room" and not self.show_comms_rooms:
                continue
            if kind in {
                "location",
                "distributed_equipment_room",
                "mer",
                "main_equipment_room",
                "polan",
                "polan_location",
            } and not self.show_locations:
                continue
            d = math.hypot(float(point.get("x", 0.0)) - x, float(point.get("y", 0.0)) - y)
            if d <= best_dist:
                best_dist = d
                best_name = str(name)
        for department_id, dept in snapshot.departments.items():
            d = math.hypot(float(dept.get("x", 0.0)) - x, float(dept.get("y", 0.0)) - y)
            if d <= best_dist:
                best_dist = d
                best_name = str(department_id)
        if self.show_network:
            for instance_id, instance in snapshot.network_instances.items():
                asset = snapshot.network_assets.get(str(instance.get("asset_id", "")).strip(), {})
                asset_type = str(asset.get("asset_type", "")).strip().lower()
                asset_name = str(asset.get("name", "")).strip().lower()
                if asset_type in {"patch_panel", "cable_management", "cable_manager"} or "cable management" in asset_name or "cable-management" in asset_name:
                    continue
                d = math.hypot(float(instance.get("x", 0.0)) - x, float(instance.get("y", 0.0)) - y)
                if d <= best_dist:
                    best_dist = d
                    best_name = str(instance_id)
        return best_name

    # ------------------------------------------------------------------
    # Qt events and input coalescing
    # ------------------------------------------------------------------
    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if self._quick_mode:
            self._frame_timer.stop()
            self._resize_settle_timer.start(self._resize_settle_ms)
        else:
            self.request_redraw(DirtyLayer.ALL)

    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._quick_mode:
            if not self._screen_change_connected:
                try:
                    top_level = self.window().windowHandle()
                    if top_level is not None:
                        top_level.screenChanged.connect(self._screen_changed)
                        self._screen_change_connected = True
                except (AttributeError, RuntimeError):
                    pass
            self._resize_settle_timer.start(0)
        else:
            self.request_redraw(DirtyLayer.ALL)

    def paintGL(self) -> None:  # OpenGL compatibility fallback.
        if self._quick_mode:
            return
        painter = QPainter(self)
        try:
            self._paint_layer(painter, DirtyLayer.DXF)
            self._paint_layer(painter, DirtyLayer.EDGES)
            self._paint_layer(painter, DirtyLayer.OBJECTS)
            self._paint_layer(painter, DirtyLayer.OVERLAY)
        finally:
            painter.end()

    def mousePressEvent(self, event) -> None:
        x, y = self.screen_to_world(event.position())
        if event.button() == Qt.LeftButton:
            self.leftClicked.emit(event, x, y)
        elif event.button() == Qt.RightButton:
            # Opening a context menu is not a visual change. In particular, do
            # not enter fast-interaction mode: its completion timer previously
            # forced a full DXF/graph repaint after every right click.
            self.rightClicked.emit(event, x, y)
        elif event.button() == Qt.MiddleButton:
            self._last_middle_pos = event.position().toPoint()
            # Panning is a pure retained-texture transform.  Cancel any
            # outstanding zoom/drag quality pass as well, otherwise its timer
            # can fire during the pan and reset the camera unexpectedly.
            self._quality_timer.stop()
            self._fast_interaction = False
            self._interaction_finish_layers = 0
            self.middleClicked.emit(event)
        event.accept()

    def mouseDoubleClickEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            x, y = self.screen_to_world(event.position())
            self.leftDoubleClicked.emit(event, x, y)
        event.accept()

    def mouseReleaseEvent(self, event) -> None:
        if event.button() == Qt.LeftButton:
            self._emit_pending_drag()
            self.leftReleased.emit(event)
        elif event.button() == Qt.MiddleButton:
            self._last_middle_pos = None
            self.middleReleased.emit(event)
            # The current live transform is already exact. Keep it in place on
            # ordinary releases. Only a gesture that exhausted the enlarged
            # 1920 x 1080 based overscan schedules a deferred geometry rebake.
            if self._pan_rebake_pending:
                self._pan_rebake_pending = False
                QTimer.singleShot(0, lambda: self.request_redraw(DirtyLayer.VIEW))
            event.accept()
            return
        if self._fast_interaction:
            self._quality_timer.start(100)
        event.accept()

    def mouseMoveEvent(self, event) -> None:
        x, y = self.screen_to_world(event.position())
        self.mouseMoved.emit(event, float(x), float(y))
        if event.buttons() & Qt.MiddleButton and self._last_middle_pos is not None:
            current = event.position().toPoint()
            delta = current - self._last_middle_pos
            self._offset += QPointF(delta.x(), delta.y())
            self._last_middle_pos = current
            self.middleDragged.emit(event)
            self._apply_live_view_transform()
            event.accept()
            return

        if event.buttons() & Qt.LeftButton:
            self._pending_drag = (_PointerEventSnapshot(event), x, y)
            elapsed_ms = (time.monotonic() - self._last_drag_emit) * 1000.0
            if elapsed_ms >= self._frame_interval_ms:
                self._emit_pending_drag()
            elif not self._drag_timer.isActive():
                self._drag_timer.start(max(1, int(self._frame_interval_ms - elapsed_ms)))
            # Point/rubber-band dragging changes graph objects, not the
            # camera. Restore full object/edge quality after the drag without
            # unnecessarily repainting the DXF background.
            self._mark_interaction(DirtyLayer.EDGES | DirtyLayer.OBJECTS)
        event.accept()

    def _emit_pending_drag(self) -> None:
        if self._pending_drag is None:
            return
        event, x, y = self._pending_drag
        self._pending_drag = None
        self._last_drag_emit = time.monotonic()
        self.mouseDragged.emit(event, float(x), float(y))

    def wheelEvent(self, event) -> None:
        old_world = self.screen_to_world(event.position())
        delta = event.angleDelta().y()
        if delta == 0:
            return
        # Smooth trackpads can generate a large stream; use an exponential factor.
        factor = math.pow(1.0015, float(delta))
        factor = max(0.65, min(1.55, factor))
        self._scale = max(0.001, min(5000.0, self._scale * factor))
        new_screen = self.world_to_screen(old_world[0], old_world[1])
        self._offset += QPointF(event.position().x(), event.position().y()) - new_screen
        self.mouseWheelScrolled.emit(event)
        self._mark_interaction(
            DirtyLayer.VIEW,
            settle_ms=self._zoom_settle_ms,
        )
        self._apply_live_view_transform()
        event.accept()

    # ------------------------------------------------------------------
    # Layer painting
    # ------------------------------------------------------------------
    def _paint_layer(self, painter: QPainter, layer_mask: int) -> None:
        painter.setRenderHint(QPainter.Antialiasing, False)
        painter.setRenderHint(QPainter.TextAntialiasing, not self._fast_interaction)

        if layer_mask == DirtyLayer.DXF:
            device = painter.device()
            paint_width = (
                float(device.width()) if device is not None else float(self.width())
            )
            paint_height = (
                float(device.height()) if device is not None else float(self.height())
            )
            painter.fillRect(
                QRectF(0, 0, paint_width, paint_height), self._background
            )
            painter.save()
            painter.translate(self._cache_margin_x, self._cache_margin_y)
            if self.show_dxf:
                self._draw_dxf(painter)
            painter.restore()
            return
        if layer_mask == DirtyLayer.EDGES:
            painter.save()
            painter.translate(self._cache_margin_x, self._cache_margin_y)
            if self.show_graph:
                self._draw_edges(painter)
                if self.show_network and self.show_network_links:
                    self._draw_network_links(painter)
            painter.restore()
            return
        if layer_mask == DirtyLayer.OBJECTS:
            painter.save()
            painter.translate(self._cache_margin_x, self._cache_margin_y)
            if self.show_graph:
                self._draw_equipment_room_placement_zones(painter)
                self._draw_equipment_room_extents(painter)
                self._draw_data_room_measurement(painter)
                self._draw_departments(painter)
                self._draw_points(painter)
                if self.show_network and self.show_network_assets:
                    self._draw_network_assets(painter)
                if self.show_network and self.show_wireless_devices:
                    self._draw_wireless_devices(painter)
            painter.restore()
            return
        if layer_mask == DirtyLayer.OVERLAY:
            if self.show_overlay and self._overlay_provider is not None:
                self._overlay_provider(painter, self.rect())

    def _apply_world_transform(self, painter: QPainter) -> None:
        painter.translate(self._offset)
        painter.scale(self._scale, self._scale)

    # ------------------------------------------------------------------
    # DXF cache and drawing
    # ------------------------------------------------------------------
    def _ensure_dxf_cache(self) -> None:
        entities = list(getattr(self.dxf_scene, "entities", []) or [])
        source_key = self._dxf_source_key(self.dxf_scene)
        if self._dxf_cache.source_key == source_key:
            return
        self._dxf_cache.clear()
        self._dxf_cache.source_key = source_key
        if not entities:
            return

        bounds = getattr(self.dxf_scene, "bounds", None)
        if bounds and len(bounds) == 4:
            min_x, min_y, max_x, max_y = [float(v) for v in bounds]
        else:
            min_x = min_y = 0.0
            max_x = max_y = 100.0
        span = max(1.0, max(max_x - min_x, max_y - min_y))
        target_axis_tiles = max(8, min(64, int(math.sqrt(max(1, len(entities))) / 2.0)))
        tile_size = max(2.0, span / float(target_axis_tiles))
        self._dxf_cache.origin_x = min_x
        self._dxf_cache.origin_y = min_y
        self._dxf_cache.tile_size = tile_size

        def tile_for(ix: int, iy: int) -> _DxfTile:
            return self._dxf_cache.tiles.setdefault((ix, iy), _DxfTile())

        for entity in entities:
            etype = str(entity.get("type", ""))
            bbox = entity.get("bbox")
            targets: List[_DxfTile]
            if bbox and len(bbox) == 4:
                bx1, by1, bx2, by2 = [float(v) for v in bbox]
                ix1 = math.floor((min(bx1, bx2) - min_x) / tile_size)
                ix2 = math.floor((max(bx1, bx2) - min_x) / tile_size)
                iy1 = math.floor((min(by1, by2) - min_y) / tile_size)
                iy2 = math.floor((max(by1, by2) - min_y) / tile_size)
                cell_count = (ix2 - ix1 + 1) * (iy2 - iy1 + 1)
                if cell_count > 36:
                    targets = [self._dxf_cache.global_tile]
                else:
                    targets = [tile_for(ix, iy) for ix in range(ix1, ix2 + 1) for iy in range(iy1, iy2 + 1)]
            else:
                targets = [self._dxf_cache.global_tile]

            if etype == "TEXT":
                text = str(entity.get("text") or "").strip()
                if not text or float(entity.get("height") or 0.0) > 40.0:
                    continue
                # Text belongs to one tile only to avoid duplicate labels.
                x, y = entity.get("insert", (0.0, 0.0))
                ix = math.floor((float(x) - min_x) / tile_size)
                iy = math.floor((float(y) - min_y) / tile_size)
                tile_for(ix, iy).text_entities.append(entity)
                continue

            for tile in targets:
                if etype == "LINE":
                    x1, y1 = entity["start"]
                    x2, y2 = entity["end"]
                    tile.line_path.moveTo(float(x1), -float(y1))
                    tile.line_path.lineTo(float(x2), -float(y2))
                elif etype == "POLYLINE":
                    pts = entity.get("points", [])
                    if len(pts) < 2:
                        continue
                    tile.poly_path.moveTo(float(pts[0][0]), -float(pts[0][1]))
                    for x, y in pts[1:]:
                        tile.poly_path.lineTo(float(x), -float(y))
                    if entity.get("closed"):
                        tile.poly_path.closeSubpath()
                elif etype == "CIRCLE":
                    cx, cy = entity["center"]
                    radius = float(entity["radius"])
                    tile.arc_path.addEllipse(
                        QRectF(float(cx) - radius, -(float(cy) + radius), radius * 2.0, radius * 2.0)
                    )
                elif etype == "ARC":
                    cx, cy = entity["center"]
                    radius = float(entity["radius"])
                    start_angle = float(entity.get("start_angle", 0.0))
                    end_angle = float(entity.get("end_angle", 0.0))
                    span_angle = end_angle - start_angle
                    if span_angle <= 0:
                        span_angle += 360.0
                    rect = QRectF(float(cx) - radius, -(float(cy) + radius), radius * 2.0, radius * 2.0)
                    tile.arc_path.arcMoveTo(rect, -start_angle)
                    tile.arc_path.arcTo(rect, -start_angle, -span_angle)

    def _visible_dxf_tiles(self) -> Iterable[_DxfTile]:
        self._ensure_dxf_cache()
        cache = self._dxf_cache
        if not cache.tiles:
            return []
        min_x, min_y, max_x, max_y = self.visible_world_bounds(100.0)
        ix1 = math.floor((min_x - cache.origin_x) / cache.tile_size) - 1
        ix2 = math.floor((max_x - cache.origin_x) / cache.tile_size) + 1
        iy1 = math.floor((min_y - cache.origin_y) / cache.tile_size) - 1
        iy2 = math.floor((max_y - cache.origin_y) / cache.tile_size) + 1
        return [cache.tiles[(ix, iy)] for ix in range(ix1, ix2 + 1) for iy in range(iy1, iy2 + 1) if (ix, iy) in cache.tiles]

    def _draw_dxf(self, painter: QPainter) -> None:
        if self.dxf_scene is None or not getattr(self.dxf_scene, "entities", None):
            return
        self._ensure_dxf_cache()
        visible_tiles = list(self._visible_dxf_tiles())
        geometry_tiles = [self._dxf_cache.global_tile] + visible_tiles
        painter.save()
        self._apply_world_transform(painter)
        painter.setBrush(Qt.NoBrush)
        for tile in geometry_tiles:
            if not tile.line_path.isEmpty():
                painter.setPen(self._dxf_line_pen)
                painter.drawPath(tile.line_path)
            if not tile.poly_path.isEmpty():
                painter.setPen(self._dxf_poly_pen)
                painter.drawPath(tile.poly_path)
            if not tile.arc_path.isEmpty():
                painter.setPen(self._dxf_arc_pen)
                painter.drawPath(tile.arc_path)
        painter.restore()
        if self.show_labels and not self._fast_interaction:
            self._draw_dxf_text(painter, visible_tiles)

    def _draw_dxf_text(self, painter: QPainter, tiles: Sequence[_DxfTile]) -> None:
        """Draw DXF text at a fixed model-space height.

        No font size is derived from the current camera scale.  The same world
        geometry is painted at every zoom level and the camera transform alone
        determines its apparent screen size.
        """
        count = 0
        painter.save()
        self._apply_world_transform(painter)
        try:
            for tile in tiles:
                for entity in tile.text_entities:
                    if count >= self._max_dxf_labels:
                        return
                    text = str(entity.get("text") or "").strip()
                    if not text:
                        continue
                    x, y = entity.get("insert", (0.0, 0.0))
                    screen = self.world_to_screen(float(x), float(y))
                    if not self._cached_screen_rect(160.0).contains(screen):
                        continue
                    raw_height = float(entity.get("height") or 0.0)
                    world_height = self._dxf_label_world_scale * max(
                        0.45,
                        min(3.0, raw_height if raw_height > 0.0 else 0.8),
                    )
                    # Tiny projected labels are skipped, never resized.
                    if world_height * self._scale < 0.35:
                        continue
                    self._draw_world_text(
                        painter,
                        self.world_to_scene(float(x), float(y)),
                        text,
                        QColor("#C0C0C0"),
                        world_height=world_height,
                        rotation=-float(entity.get("rotation", 0.0)),
                    )
                    count += 1
        finally:
            painter.restore()

    # ------------------------------------------------------------------
    # Graph drawing
    # ------------------------------------------------------------------
    def _ensure_frame_snapshot(self) -> _FrameSnapshot:
        if self._frame_snapshot is not None and self._frame_snapshot.floor == int(self.floor):
            return self._frame_snapshot
        if self.store is None:
            snapshot = _FrameSnapshot(int(self.floor), {}, {}, {}, {}, {}, {}, {})
            self._frame_snapshot = snapshot
            return snapshot
        data = getattr(self.store, "data", {}) or {}
        all_points = self.store.all_points() if hasattr(self.store, "all_points") else {}
        floor_points = self.store.points_for_floor(self.floor) if hasattr(self.store, "points_for_floor") else {}
        departments = self.store.departments_for_floor(self.floor) if hasattr(self.store, "departments_for_floor") else {}
        network_instances = network_instances_for_floor(data, self.floor)
        all_network_instances = {
            str(item.get("id", "")).strip(): item
            for item in data.get("network_asset_instances", [])
            if isinstance(item, dict) and str(item.get("id", "")).strip()
        }
        network_assets = {
            str(item.get("id", "")).strip(): item
            for item in data.get("network_assets", [])
            if isinstance(item, dict) and str(item.get("id", "")).strip()
        }
        snapshot = _FrameSnapshot(
            int(self.floor),
            all_points,
            floor_points,
            departments,
            data,
            network_instances,
            all_network_instances,
            network_assets,
        )
        self._frame_snapshot = snapshot
        return snapshot

    @staticmethod
    def _inside_bounds(record: dict, bounds: Bounds, padding: float = 0.0) -> bool:
        x = float(record.get("x", 0.0))
        y = float(record.get("y", 0.0))
        return bounds[0] - padding <= x <= bounds[2] + padding and bounds[1] - padding <= y <= bounds[3] + padding

    @staticmethod
    def _segment_visible(a: dict, b: dict, bounds: Bounds) -> bool:
        ax, ay = float(a.get("x", 0.0)), float(a.get("y", 0.0))
        bx, by = float(b.get("x", 0.0)), float(b.get("y", 0.0))
        return not (
            max(ax, bx) < bounds[0]
            or min(ax, bx) > bounds[2]
            or max(ay, by) < bounds[1]
            or min(ay, by) > bounds[3]
        )

    def _draw_edges(self, painter: QPainter) -> None:
        if self.store is None or not self.show_edges:
            return
        snapshot = self._ensure_frame_snapshot()
        bounds = self.visible_world_bounds(80.0)
        same_path = QPainterPath()
        cross_path = QPainterPath()
        for edge in snapshot.data.get("corridors", {}).get("edges", []):
            a = snapshot.all_points.get(edge.get("from"))
            b = snapshot.all_points.get(edge.get("to"))
            if not a or not b:
                continue
            a_floor = int(a.get("floor", 0))
            b_floor = int(b.get("floor", 0))
            if self.floor not in {a_floor, b_floor} or not self._segment_visible(a, b, bounds):
                continue
            pa = self.world_to_scene(float(a.get("x", 0.0)), float(a.get("y", 0.0)))
            pb = self.world_to_scene(float(b.get("x", 0.0)), float(b.get("y", 0.0)))
            path = cross_path if a_floor != b_floor else same_path
            path.moveTo(pa)
            path.lineTo(pb)
        painter.save()
        self._apply_world_transform(painter)
        if not same_path.isEmpty():
            painter.setPen(self._same_floor_edge_pen)
            painter.drawPath(same_path)
        if not cross_path.isEmpty():
            painter.setPen(self._cross_floor_edge_pen)
            painter.drawPath(cross_path)
        painter.restore()

    def _draw_departments(self, painter: QPainter) -> None:
        if not self.show_departments:
            return
        snapshot = self._ensure_frame_snapshot()
        bounds = self.visible_world_bounds(60.0)
        labels: List[Tuple[QPointF, str, QColor]] = []
        painter.save()
        self._apply_world_transform(painter)
        for department_id, dept in snapshot.departments.items():
            if not self._inside_bounds(dept, bounds, 2.0):
                continue
            pos = self.world_to_scene(float(dept.get("x", 0.0)), float(dept.get("y", 0.0)))
            selected = str(department_id) == str(self.selected_point_name)
            poly = QPolygonF([
                QPointF(pos.x(), pos.y() - 0.7),
                QPointF(pos.x() + 0.7, pos.y()),
                QPointF(pos.x(), pos.y() + 0.7),
                QPointF(pos.x() - 0.7, pos.y()),
            ])
            painter.setBrush(QBrush(QColor("#1abc9c")))
            painter.setPen(QPen(QColor("#ffffff") if selected else QColor("#8ef3df"), 0.08))
            painter.drawPolygon(poly)
            if self.show_labels:
                labels.append((self.world_to_scene(float(dept.get("x", 0.0)), float(dept.get("y", 0.0))), str(dept.get("name") or department_id), QColor("#aaf7ea")))
        painter.restore()
        self._draw_label_batch(painter, labels)

    def _draw_equipment_room_placement_zones(self, painter: QPainter) -> None:
        if not self.show_placement_zones:
            return
        snapshot = self._ensure_frame_snapshot()
        zones = [
            zone
            for zone in snapshot.data.get("equipment_room_placement_zones", [])
            if isinstance(zone, dict)
            and int(zone.get("floor", 0)) == int(self.floor)
        ]
        preview = self.placement_zone_preview
        if (
            isinstance(preview, dict)
            and int(preview.get("floor", 0)) == int(self.floor)
        ):
            zones.append(preview)
        if not zones:
            return
        visible = self.visible_world_bounds(20.0)
        labels: List[Tuple[QPointF, str, QColor]] = []
        painter.save()
        self._apply_world_transform(painter)
        for zone in zones:
            is_preview = bool(zone.get("preview", False))
            min_x = float(zone.get("min_x", 0.0))
            max_x = float(zone.get("max_x", 0.0))
            min_y = float(zone.get("min_y", 0.0))
            max_y = float(zone.get("max_y", 0.0))
            if (
                max_x < visible[0]
                or min_x > visible[2]
                or max_y < visible[1]
                or min_y > visible[3]
            ):
                continue
            allow_comms = bool(zone.get("allow_comms_room", False))
            allow_der = bool(zone.get("allow_distributed_equipment_room", False))
            if allow_comms and allow_der:
                colour = QColor("#35a7ff")
                allowance = "CR + DER"
            elif allow_comms:
                colour = QColor("#18c37e")
                allowance = "CR"
            else:
                colour = QColor("#ffb347")
                allowance = "DER"
            fill = QColor(colour)
            fill.setAlpha(48 if is_preview else 28)
            selected = (
                str(zone.get("id", "")).strip()
                == str(self.selected_placement_zone_id or "").strip()
            )
            pen = QPen(
                QColor("#ffffff") if selected or is_preview else colour,
                0.18 if is_preview else (0.16 if selected else 0.12),
                Qt.DashLine if is_preview else (Qt.SolidLine if selected else Qt.DashLine),
            )
            painter.setPen(pen)
            painter.setBrush(QBrush(fill))
            rect = QRectF(min_x, -max_y, max_x - min_x, max_y - min_y)
            painter.drawRect(rect)
            if selected:
                handle_radius = max(0.06, 5.0 / max(self._scale, 0.001))
                painter.setPen(QPen(colour, 0.08))
                painter.setBrush(QBrush(QColor("#ffffff")))
                mid_x = (min_x + max_x) / 2.0
                mid_y = (min_y + max_y) / 2.0
                for hx, hy in (
                    (min_x, min_y),
                    (mid_x, min_y),
                    (max_x, min_y),
                    (max_x, mid_y),
                    (max_x, max_y),
                    (mid_x, max_y),
                    (min_x, max_y),
                    (min_x, mid_y),
                ):
                    painter.drawRect(
                        QRectF(
                            hx - handle_radius,
                            -hy - handle_radius,
                            handle_radius * 2.0,
                            handle_radius * 2.0,
                        )
                    )
            labels.append(
                (
                    self.world_to_scene(min_x, max_y),
                    (
                        "Indicative placement area"
                        if is_preview
                        else (
                            f"{zone.get('name') or zone.get('id') or 'Placement zone'} "
                            f"[{allowance}; CR "
                            f"{zone.get('max_comms_rooms', 0) or 'unlimited'}; DER "
                            f"{zone.get('max_distributed_equipment_rooms', 0) or 'unlimited'}]"
                        )
                    ),
                    QColor("#ffffff") if is_preview else colour,
                )
            )
        painter.restore()
        self._draw_label_batch(painter, labels)

    def _draw_equipment_room_extents(self, painter: QPainter) -> None:
        overlay = self.equipment_room_extent_overlay
        if not isinstance(overlay, dict):
            return
        if int(overlay.get("floor", 0)) != int(self.floor):
            return

        x = float(overlay.get("x", 0.0))
        y = float(overlay.get("y", 0.0))
        distance_limit = max(0.0, float(overlay.get("distance_limit_m", 0.0)))
        possible_polylines = list(overlay.get("possible_polylines", []) or [])
        current_polylines = list(overlay.get("current_polylines", []) or [])
        boundary_polyline = list(overlay.get("boundary_polyline", []) or [])
        if not possible_polylines and not current_polylines:
            return

        def build_path(polylines) -> QPainterPath:
            path = QPainterPath()
            for polyline in polylines:
                if not isinstance(polyline, (list, tuple)) or len(polyline) < 2:
                    continue
                first = polyline[0]
                if not isinstance(first, (list, tuple)) or len(first) < 2:
                    continue
                path.moveTo(float(first[0]), -float(first[1]))
                for point in polyline[1:]:
                    if isinstance(point, (list, tuple)) and len(point) >= 2:
                        path.lineTo(float(point[0]), -float(point[1]))
            return path

        possible_colour = QColor("#35a7ff")
        current_colour = QColor("#18c37e")
        painter.save()
        self._apply_world_transform(painter)
        possible_path = build_path(possible_polylines)
        if not possible_path.isEmpty():
            painter.setPen(QPen(possible_colour, 0.24, Qt.SolidLine))
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(possible_path)
        current_path = build_path(current_polylines)
        if not current_path.isEmpty():
            painter.setPen(QPen(current_colour, 0.32, Qt.SolidLine))
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(current_path)
        boundary_path = build_path([boundary_polyline])
        if not boundary_path.isEmpty():
            boundary_pen = QPen(QColor("#8bd3ff"), 0.28, Qt.DashLine)
            painter.setPen(boundary_pen)
            painter.setBrush(Qt.NoBrush)
            painter.drawPath(boundary_path)
        painter.restore()

        name = str(overlay.get("name", "Proposed equipment room")).strip()
        label = f"{name}: graph extent {distance_limit:.1f} m"
        if current_polylines:
            label += " | current routes"
        self._draw_label_batch(
            painter,
            [(self.world_to_scene(x, y), label, possible_colour)],
        )

    def _draw_data_room_measurement(self, painter: QPainter) -> None:
        overlay = self.data_room_measurement_overlay
        if not isinstance(overlay, dict):
            return
        floor_points = [
            point
            for point in overlay.get("path_points", []) or []
            if isinstance(point, dict)
            and int(point.get("floor", 0)) == int(self.floor)
        ]
        if not floor_points:
            return

        colour = QColor(
            "#198754" if bool(overlay.get("within_limit", False)) else "#dc3545"
        )
        path = QPainterPath()
        active = False
        previous_floor = None
        for point in overlay.get("path_points", []) or []:
            if not isinstance(point, dict):
                active = False
                previous_floor = None
                continue
            point_floor = int(point.get("floor", 0))
            if point_floor != int(self.floor):
                active = False
                previous_floor = point_floor
                continue
            px = float(point.get("x", 0.0))
            py = -float(point.get("y", 0.0))
            if not active or previous_floor != point_floor:
                path.moveTo(px, py)
            else:
                path.lineTo(px, py)
            active = True
            previous_floor = point_floor

        painter.save()
        self._apply_world_transform(painter)
        if not path.isEmpty():
            painter.setBrush(Qt.NoBrush)
            painter.setPen(QPen(QColor("#ffffff"), 0.85, Qt.SolidLine))
            painter.drawPath(path)
            painter.setPen(QPen(colour, 0.42, Qt.SolidLine))
            painter.drawPath(path)
        painter.restore()

        label_point = floor_points[len(floor_points) // 2]
        total = float(overlay.get("total_distance_m", 0.0) or 0.0)
        routed = float(overlay.get("routed_distance_m", 0.0) or 0.0)
        extension = float(overlay.get("extension_distance_m", 0.0) or 0.0)
        limit = float(overlay.get("distance_limit_m", 0.0) or 0.0)
        outcome = "within limit" if overlay.get("within_limit") else "OVER LIMIT"
        label = (
            f"{overlay.get('data_point_name', '')} to "
            f"{overlay.get('room_name', '')}: {total:.2f} m "
            f"(route {routed:.2f} + extension {extension:.2f}) | "
            f"limit {limit:.2f} m - {outcome}"
        )
        self._draw_label_batch(
            painter,
            [
                (
                    self.world_to_scene(
                        float(label_point.get("x", 0.0)),
                        float(label_point.get("y", 0.0)),
                    ),
                    label,
                    colour,
                )
            ],
        )

    def _draw_points(self, painter: QPainter) -> None:
        snapshot = self._ensure_frame_snapshot()
        bounds = self.visible_world_bounds(60.0)
        labels: List[Tuple[QPointF, str, QColor]] = []
        painter.save()
        self._apply_world_transform(painter)
        for name, point in snapshot.floor_points.items():
            if not self._inside_bounds(point, bounds, 2.0):
                continue
            kind = str(point.get("kind", "")).strip()
            if kind == "corridor_node" and not self.show_nodes:
                continue
            if kind == "data_point" and not self.show_data_points:
                continue
            if (
                kind == "data_point"
                and self.show_unconnected_data_points_only
                and str(name) not in self.unconnected_data_point_names
            ):
                continue
            if (
                kind == "data_point"
                and self.show_routing_unconnected_data_points_only
                and str(name) not in self.routing_unconnected_data_point_names
            ):
                continue
            if (
                kind == "data_point"
                and self.hide_connected_data_points
                and str(name) in self.connected_data_point_names
            ):
                continue
            if kind == "comms_room" and not self.show_comms_rooms:
                continue
            if kind in {"location", "distributed_equipment_room", "mer", "main_equipment_room", "polan", "polan_location"} and not self.show_locations:
                continue

            pos = self.world_to_scene(float(point.get("x", 0.0)), float(point.get("y", 0.0)))
            selected = str(name) == str(self.selected_point_name) or str(name) in self.selected_template_names
            outline = QPen(QColor("#ffffff") if selected else QColor("transparent"), 0.0)
            if kind in {"location", "comms_room"}:
                painter.setPen(outline)
                painter.setBrush(QBrush(QColor("#18c37e")))
                r = 0.3
                painter.drawEllipse(QRectF(pos.x() - r, pos.y() - r, r * 2.0, r * 2.0))
                label_color = QColor("#9bf0cd")
            elif kind == "corridor_node":
                painter.setPen(outline)
                painter.setBrush(QBrush(QColor("#ff6b6b") if point.get("restricted", False) else QColor("#f2c94c")))
                r = 0.3
                painter.drawRect(QRectF(pos.x() - r, pos.y() - r, r * 2.0, r * 2.0))
                label_color = QColor("#ffe8a3")
            elif kind == "data_point":
                self._draw_diamond(
                    painter,
                    pos,
                    0.45,
                    QColor("#b07cff"),
                    QColor("#ffffff") if selected else QColor("#d5bbff"),
                )
                label_color = QColor("#eadcff")
            else:
                self._draw_diamond(painter, pos, 0.5, QColor("#ff7b72"), QColor("#ffffff") if selected else QColor("#ffb3ae"))
                label_color = QColor("#ffb3ae")
            if self.show_labels:
                labels.append((self.world_to_scene(float(point.get("x", 0.0)), float(point.get("y", 0.0))), str(name), label_color))
        painter.restore()
        self._draw_label_batch(painter, labels)

    @staticmethod
    def _is_active_graph_device(instance: dict, assets: Dict[str, dict]) -> bool:
        asset = assets.get(str(instance.get("asset_id", "")).strip(), {})
        asset_type = str(asset.get("asset_type", "")).strip().lower()
        asset_name = str(asset.get("name", "")).strip().lower()
        role = str(instance.get("design_role", "")).strip().lower()
        passive_or_power_tokens = (
            "patch", "splitter", "coupler", "adapter", "splice",
            "cable management", "cable-management", "cable manager",
            "ups", "pdu", "power distribution", "power supply",
            "battery", "rectifier", "shelf", "blanking panel",
        )
        if asset_type in {"patch_panel", "fibre_splitter", "cable_management", "cable_manager", "ups", "pdu", "power_device"}:
            return False
        if any(token in asset_name or token in role for token in passive_or_power_tokens):
            return False
        if asset_type in {"network_router", "firewall", "network_switch", "wireless_access_point", "wireless_device", "optical_line_terminal", "optical_network_terminal"}:
            return True
        return any(token in role for token in ("core", "distribution", "aggregation", "access", "gateway", "router", "firewall", "olt", "ont", "wireless", "client"))

    def _draw_network_links(self, painter: QPainter) -> None:
        snapshot = self._ensure_frame_snapshot()
        floor = int(self.floor)
        bounds = self.visible_world_bounds(80.0)
        fibre_path = QPainterPath()
        copper_path = QPainterPath()
        failover_path = QPainterPath()

        def append_records(path: QPainterPath, records: Sequence[dict]) -> None:
            for a, b in zip(records, records[1:]):
                try:
                    if int(a.get("floor", floor)) != floor or int(b.get("floor", floor)) != floor:
                        continue
                    if not self._segment_visible(a, b, bounds):
                        continue
                    pa = self.world_to_scene(float(a.get("x", 0.0)), float(a.get("y", 0.0)))
                    pb = self.world_to_scene(float(b.get("x", 0.0)), float(b.get("y", 0.0)))
                    path.moveTo(pa)
                    path.lineTo(pb)
                except (TypeError, ValueError):
                    continue

        for connection in snapshot.data.get("network_connections", []):
            if not isinstance(connection, dict):
                continue
            source = snapshot.all_network_instances.get(str(connection.get("from_instance_id", "")).strip())
            target = snapshot.all_network_instances.get(str(connection.get("to_instance_id", "")).strip())
            if not source or not target:
                continue
            if not self._is_active_graph_device(source, snapshot.network_assets) or not self._is_active_graph_device(target, snapshot.network_assets):
                continue
            route_records = [snapshot.all_points[name] for name in connection.get("route_path", []) if str(name) in snapshot.all_points]
            records = [source] + route_records + [target]
            medium = str(connection.get("medium", "") or "").strip().lower()
            standby = bool(connection.get("standby", False)) or str(connection.get("redundancy_role", "")).lower() in {"secondary", "standby"}
            path = failover_path if standby else (fibre_path if medium == "fibre" else copper_path)
            append_records(path, records)

        for assignment in snapshot.data.get("network_endpoint_assignments", []):
            if not isinstance(assignment, dict):
                continue
            instance = snapshot.all_network_instances.get(str(assignment.get("network_instance_id", "")).strip())
            endpoint = snapshot.all_points.get(str(assignment.get("endpoint_name", "")).strip())
            if not instance or not endpoint:
                continue
            route_records = [snapshot.all_points[name] for name in assignment.get("route_path", []) if str(name) in snapshot.all_points]
            append_records(copper_path, [instance] + route_records + [endpoint])

        painter.save()
        self._apply_world_transform(painter)
        if not fibre_path.isEmpty():
            painter.setPen(QPen(QColor("#6f8dff"), 0.08))
            painter.drawPath(fibre_path)
        if not copper_path.isEmpty():
            painter.setPen(QPen(QColor("#4fbfa3"), 0.06))
            painter.drawPath(copper_path)
        if not failover_path.isEmpty():
            painter.setPen(QPen(QColor("#d68f52"), 0.07, Qt.DashLine))
            painter.drawPath(failover_path)
        painter.restore()

    @staticmethod
    def _is_wireless_layer_instance(instance: dict, asset: dict) -> bool:
        return bool(
            str(asset.get("asset_type", "")).strip().lower() in {"wireless_device", "wireless_access_point"}
            or instance.get("wireless_device_layer", False)
            or instance.get("imported_wireless_device", False)
        )

    def _draw_network_assets(self, painter: QPainter) -> None:
        snapshot = self._ensure_frame_snapshot()
        bounds = self.visible_world_bounds(60.0)
        labels: List[Tuple[QPointF, str, QColor]] = []
        painter.save()
        self._apply_world_transform(painter)
        for instance_id, instance in snapshot.network_instances.items():
            if not self._inside_bounds(instance, bounds, 2.0):
                continue
            asset = snapshot.network_assets.get(str(instance.get("asset_id", "")).strip(), {})
            asset_type = str(asset.get("asset_type", "") or "").strip().lower()
            if self._is_wireless_layer_instance(instance, asset):
                continue
            if asset_type == "network_switch" or not self._is_active_graph_device(instance, snapshot.network_assets):
                continue
            pos = self.world_to_scene(float(instance.get("x", 0.0)), float(instance.get("y", 0.0)))
            selected = str(instance_id) == str(self.selected_point_name)
            fill = {
                "network_switch": QColor("#265f88"),
                "network_router": QColor("#7259a7"),
                "firewall": QColor("#a04f4f"),
                "optical_line_terminal": QColor("#4a5fa8"),
                "optical_network_terminal": QColor("#2d806d"),
                "fibre_splitter": QColor("#697f3f"),
                "patch_panel": QColor("#53616d"),
            }.get(asset_type, QColor("#53616d"))
            outline = QColor("#ffffff") if selected else QColor("#cce7ff")
            role = str(instance.get("design_role", "") or "").lower()
            if asset_type == "fibre_splitter" or "splitter" in role:
                self._draw_splitter_symbol(painter, pos, fill, outline)
            else:
                self._draw_diamond(painter, pos, 0.62, fill, outline)
            if self.show_labels:
                labels.append((self.world_to_scene(float(instance.get("x", 0.0)), float(instance.get("y", 0.0))), str(instance.get("name") or instance_id), QColor("#cce7ff")))
        painter.restore()
        self._draw_label_batch(painter, labels)


    def _draw_wireless_devices(self, painter: QPainter) -> None:
        snapshot = self._ensure_frame_snapshot()
        bounds = self.visible_world_bounds(60.0)
        labels: List[Tuple[QPointF, str, QColor]] = []
        painter.save()
        self._apply_world_transform(painter)
        for instance_id, instance in snapshot.network_instances.items():
            if not self._inside_bounds(instance, bounds, 2.0):
                continue
            asset = snapshot.network_assets.get(str(instance.get("asset_id", "")).strip(), {})
            if not self._is_wireless_layer_instance(instance, asset):
                continue
            pos = self.world_to_scene(float(instance.get("x", 0.0)), float(instance.get("y", 0.0)))
            selected = str(instance_id) == str(self.selected_point_name)
            category = str(asset.get("wireless_device_category", "") or "").strip().lower()
            fill = QColor("#bc7a24") if category == "access_point" else QColor("#8f5db7")
            outline = QColor("#ffffff") if selected else QColor("#ffe1a8")
            self._draw_wireless_symbol(painter, pos, fill, outline)
            if self.show_labels:
                labels.append((
                    self.world_to_scene(float(instance.get("x", 0.0)), float(instance.get("y", 0.0))),
                    str(instance.get("name") or instance_id),
                    QColor("#ffe1a8"),
                ))
        painter.restore()
        self._draw_label_batch(painter, labels)

    @staticmethod
    def _draw_wireless_symbol(
        painter: QPainter, pos: QPointF, fill: QColor, outline: QColor
    ) -> None:
        """Draw a dedicated radio-beacon symbol for the wireless layer."""
        painter.setPen(QPen(outline, 0.08))
        painter.setBrush(QBrush(fill))
        painter.drawEllipse(QRectF(pos.x() - 0.14, pos.y() - 0.14, 0.28, 0.28))
        painter.drawLine(QPointF(pos.x(), pos.y() + 0.14), QPointF(pos.x(), pos.y() + 0.58))
        painter.setBrush(Qt.NoBrush)
        for radius in (0.38, 0.62):
            rect = QRectF(pos.x() - radius, pos.y() - radius, radius * 2.0, radius * 2.0)
            painter.drawArc(rect, 28 * 16, 124 * 16)

    @staticmethod
    def _draw_splitter_symbol(painter: QPainter, pos: QPointF, fill: QColor, outline: QColor) -> None:
        radius = 0.58
        rect = QRectF(pos.x() - radius, pos.y() - radius, radius * 2.0, radius * 2.0)
        painter.setBrush(QBrush(fill))
        painter.setPen(QPen(outline, 0.08))
        painter.drawRoundedRect(rect, 0.12, 0.12)
        arm = 0.34
        painter.drawLine(QPointF(pos.x() - arm, pos.y()), QPointF(pos.x() + arm, pos.y()))
        painter.drawLine(QPointF(pos.x(), pos.y() - arm), QPointF(pos.x(), pos.y() + arm))

    @staticmethod
    def _draw_diamond(painter: QPainter, pos: QPointF, radius: float, fill: QColor, outline: QColor) -> None:
        poly = QPolygonF([
            QPointF(pos.x(), pos.y() - radius),
            QPointF(pos.x() + radius, pos.y()),
            QPointF(pos.x(), pos.y() + radius),
            QPointF(pos.x() - radius, pos.y()),
        ])
        painter.setBrush(QBrush(fill))
        painter.setPen(QPen(outline, 0.08))
        painter.drawPolygon(poly)

    def _draw_label_batch(
        self,
        painter: QPainter,
        labels: Sequence[Tuple[QPointF, str, QColor]],
    ) -> None:
        """Draw graph labels as fixed model-space geometry.

        ``labels`` contains scene positions, not screen positions. Font size is
        never recalculated from zoom. The fixed model-space glyph becomes larger
        on screen only because the camera is closer.
        """
        if not self.show_labels:
            return
        limit = min(
            self._max_graph_labels,
            250 if self._fast_interaction else self._max_graph_labels,
        )
        world_height = self._graph_label_world_height
        if world_height * self._scale < 0.35:
            return
        painter.save()
        self._apply_world_transform(painter)
        try:
            for index, (scene_pos, text, color) in enumerate(labels):
                if index >= limit:
                    break
                screen = QPointF(
                    scene_pos.x() * self._scale + self._offset.x(),
                    scene_pos.y() * self._scale + self._offset.y(),
                )
                if not self._cached_screen_rect(160.0).contains(screen):
                    continue
                self._draw_world_text(
                    painter,
                    scene_pos,
                    text,
                    color,
                    world_height=world_height,
                    offset=QPointF(0.42, -0.42),
                )
        finally:
            painter.restore()

    def _draw_world_text(
        self,
        painter: QPainter,
        scene_pos: QPointF,
        text: str,
        color: QColor,
        *,
        world_height: float,
        rotation: float = 0.0,
        offset: QPointF = QPointF(0.0, 0.0),
        bold: bool = False,
    ) -> None:
        """Paint sharp vector text at a constant scene/model-space height.

        The glyph outline and its model-space height are independent of camera
        zoom. The camera transform is therefore the only operation that changes
        the apparent screen size. Using a cached QPainterPath avoids scaling a
        small pre-rasterised QStaticText bitmap, which was the source of the
        blurred labels at settled zoom levels.
        """
        if not text or world_height <= 0.0:
            return

        glyph_path, glyph_bounds = self._vector_text_path(text, bold)
        glyph_height = max(1.0e-6, float(glyph_bounds.height()))
        local_scale = float(world_height) / glyph_height

        painter.save()
        painter.translate(scene_pos + offset)
        if rotation:
            painter.rotate(float(rotation))
        painter.scale(local_scale, local_scale)
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setRenderHint(QPainter.TextAntialiasing, True)
        painter.setPen(Qt.NoPen)
        painter.setBrush(QBrush(color))
        painter.drawPath(glyph_path)
        painter.restore()

    def _vector_text_path(
        self, text: str, bold: bool = False
    ) -> Tuple[QPainterPath, QRectF]:
        """Return a normalised, cached Arial glyph outline for ``text``."""
        key = (str(text), bool(bold))
        cached = self._vector_text_cache.get(key)
        if cached is not None:
            return cached

        font = QFont("Arial")
        font.setPixelSize(96)
        font.setBold(bool(bold))
        try:
            font.setHintingPreference(QFont.PreferNoHinting)
        except Exception:
            pass

        raw_path = QPainterPath()
        raw_path.addText(QPointF(0.0, 0.0), font, str(text))
        raw_bounds = raw_path.boundingRect()

        if raw_path.isEmpty() or raw_bounds.isNull() or raw_bounds.height() <= 0.0:
            fallback = QPainterPath()
            fallback.addRect(QRectF(0.0, 0.0, 1.0, 1.0))
            result = (fallback, QRectF(0.0, 0.0, 1.0, 1.0))
        else:
            transform = QTransform()
            transform.translate(-raw_bounds.left(), -raw_bounds.top())
            normalised = transform.map(raw_path)
            result = (normalised, normalised.boundingRect())

        if len(self._vector_text_cache) > 12000:
            self._vector_text_cache.clear()
        self._vector_text_cache[key] = result
        return result

    def _static_text(self, text: str, pixel_size: int, bold: bool = False) -> QStaticText:
        key = (text, int(pixel_size), bool(bold))
        cached = self._static_text_cache.get(key)
        if cached is not None:
            return cached
        font = QFont("Arial")
        font.setPixelSize(int(pixel_size))
        font.setBold(bool(bold))
        static = QStaticText(text)
        static.prepare(QTransform(), font)
        if len(self._static_text_cache) > 12000:
            self._static_text_cache.clear()
        self._static_text_cache[key] = static
        return static

    def _draw_cached_text(
        self,
        painter: QPainter,
        screen_pos: QPointF,
        text: str,
        color: QColor,
        pixel_size: int,
        *,
        rotation: float = 0.0,
        bold: bool = False,
    ) -> None:
        if not text:
            return
        static = self._static_text(text, pixel_size, bold)
        painter.save()
        painter.translate(screen_pos)
        if rotation:
            painter.rotate(rotation)
        painter.setPen(color)
        painter.drawStaticText(QPointF(0.0, 0.0), static)
        painter.restore()

    # ------------------------------------------------------------------
    # Store helpers and bounds
    # ------------------------------------------------------------------
    def _all_points(self) -> Dict[str, dict]:
        return self._ensure_frame_snapshot().all_points

    def _points_for_floor(self) -> Dict[str, dict]:
        return self._ensure_frame_snapshot().floor_points

    def _departments_for_floor(self) -> Dict[str, dict]:
        return self._ensure_frame_snapshot().departments

    def _network_instances_for_floor(self) -> Dict[str, dict]:
        return self._ensure_frame_snapshot().network_instances

    def _content_bounds(self) -> Optional[Bounds]:
        bounds: List[Bounds] = []
        if self.dxf_scene is not None and getattr(self.dxf_scene, "bounds", None):
            bounds.append(tuple(float(x) for x in self.dxf_scene.bounds))
        snapshot = self._ensure_frame_snapshot()
        for records in (snapshot.floor_points, snapshot.departments, snapshot.network_instances if self.show_network else {}):
            if not records:
                continue
            xs = [float(p.get("x", 0.0)) for p in records.values()]
            ys = [float(p.get("y", 0.0)) for p in records.values()]
            bounds.append((min(xs), min(ys), max(xs), max(ys)))
        if not bounds:
            return None
        return (
            min(b[0] for b in bounds),
            min(b[1] for b in bounds),
            max(b[2] for b in bounds),
            max(b[3] for b in bounds),
        )
