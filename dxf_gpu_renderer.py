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
    from PySide6.QtQuick import QQuickPaintedItem, QQuickWindow, QSGRendererInterface
    from PySide6.QtQuickWidgets import QQuickWidget

    _QT_QUICK_AVAILABLE = True
except Exception:  # pragma: no cover - depends on the installed PySide6 build.
    QQmlComponent = None
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
        self.show_locations = True
        self.show_comms_rooms = True
        self.show_departments = True
        self.show_network = True
        self.show_network_links = True
        self.show_network_connections = True
        self.show_network_assets = True
        self.show_physical_fibre = True

        self.selected_point_name: Optional[str] = None
        self.selected_template_names: set[str] = set()
        self.edge_chain_start: Optional[str] = None

        self._scale = 1.0
        self._offset = QPointF(0.0, 0.0)
        self._last_middle_pos: Optional[QPoint] = None
        self._overlay_provider = None
        self._dxf_cache = DxfRenderCache()
        self._bound_dxf_key: Optional[Tuple[Any, ...]] = None
        self._frame_snapshot: Optional[_FrameSnapshot] = None
        self._rubber_band = QRubberBand(QRubberBand.Rectangle, self)
        self._static_text_cache: Dict[Tuple[str, int, bool], QStaticText] = {}

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

    def _resize_quick_layers(self) -> None:
        if not self._quick_mode:
            return
        width = max(1.0, float(self.width()))
        height = max(1.0, float(self.height()))
        if self._quick_root is not None:
            try:
                self._quick_root.setWidth(width)
                self._quick_root.setHeight(height)
            except Exception:
                pass
        for item in self._layer_items.values():
            item.setWidth(width)
            item.setHeight(height)

    def request_redraw(self, layers: int = DirtyLayer.ALL) -> None:
        self._dirty_layers |= int(layers)
        if not self.isVisible():
            return
        if not self._frame_timer.isActive():
            self._frame_timer.start()

    def update(self, *args, **kwargs) -> None:  # QGraphicsView compatibility
        self.request_redraw(DirtyLayer.ALL)

    def _flush_redraw(self) -> None:
        if not self.isVisible():
            self._frame_timer.stop()
            return
        layers = self._dirty_layers
        self._dirty_layers = 0
        if not layers:
            self._frame_timer.stop()
            return

        started = time.perf_counter()
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

    def _mark_interaction(self) -> None:
        self._fast_interaction = True
        self._quality_timer.start(140)

    def _end_fast_interaction(self) -> None:
        if not self._fast_interaction:
            return
        self._fast_interaction = False
        self.request_redraw(DirtyLayer.VIEW)

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
        show_locations: Optional[bool] = None,
        show_comms_rooms: Optional[bool] = None,
        show_departments: Optional[bool] = None,
        show_network: Optional[bool] = None,
        show_network_links: Optional[bool] = None,
        show_network_connections: Optional[bool] = None,
        show_network_assets: Optional[bool] = None,
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
        assign("show_labels", show_labels, DirtyLayer.ALL)
        assign("show_graph", show_graph, DirtyLayer.EDGES | DirtyLayer.OBJECTS)
        assign("show_overlay", show_overlay, DirtyLayer.OVERLAY)
        assign("show_edges", show_edges, DirtyLayer.EDGES)
        assign("show_nodes", show_nodes, DirtyLayer.OBJECTS)
        assign("show_data_points", show_data_points, DirtyLayer.OBJECTS)
        assign("show_locations", show_locations, DirtyLayer.OBJECTS)
        assign("show_comms_rooms", show_comms_rooms, DirtyLayer.OBJECTS)
        assign("show_departments", show_departments, DirtyLayer.OBJECTS)
        assign("show_network", show_network, DirtyLayer.EDGES | DirtyLayer.OBJECTS)
        link_value = show_network_connections
        if link_value is None:
            link_value = show_network_links
        assign("show_network_links", link_value, DirtyLayer.EDGES)
        self.show_network_connections = self.show_network_links
        assign("show_network_assets", show_network_assets, DirtyLayer.OBJECTS)
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

    def set_overlay_provider(self, overlay_provider) -> None:
        if overlay_provider is self._overlay_provider:
            return
        self._overlay_provider = overlay_provider
        self.request_redraw(DirtyLayer.OVERLAY)

    def invalidate_dxf_cache(self) -> None:
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
        self._mark_interaction()
        self.request_redraw(DirtyLayer.VIEW)

    def scale(self, sx: float, sy: float) -> None:
        factor = float(sx)
        centre = QPointF(self.width() / 2.0, self.height() / 2.0)
        before = self.screen_to_world(centre)
        self._scale = max(0.001, min(5000.0, self._scale * factor))
        after_screen = self.world_to_screen(before[0], before[1])
        self._offset += centre - after_screen
        self._mark_interaction()
        self.request_redraw(DirtyLayer.VIEW)

    def resetTransform(self) -> None:  # noqa: N802
        self._scale = 1.0
        self._offset = QPointF(0.0, 0.0)
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
        left = -padding_px
        top = -padding_px
        right = self.width() + padding_px
        bottom = self.height() + padding_px
        p1 = self.screen_to_world(QPointF(left, top))
        p2 = self.screen_to_world(QPointF(right, bottom))
        return min(p1[0], p2[0]), min(p1[1], p2[1]), max(p1[0], p2[0]), max(p1[1], p2[1])

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
        self._resize_quick_layers()
        self.request_redraw(DirtyLayer.ALL)

    def showEvent(self, event) -> None:
        super().showEvent(event)
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
        self._mark_interaction()
        x, y = self.screen_to_world(event.position())
        if event.button() == Qt.LeftButton:
            self.leftClicked.emit(event, x, y)
        elif event.button() == Qt.RightButton:
            self.rightClicked.emit(event, x, y)
        elif event.button() == Qt.MiddleButton:
            self._last_middle_pos = event.position().toPoint()
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
        self._quality_timer.start(100)
        event.accept()

    def mouseMoveEvent(self, event) -> None:
        if event.buttons() & Qt.MiddleButton and self._last_middle_pos is not None:
            current = event.position().toPoint()
            delta = current - self._last_middle_pos
            self._offset += QPointF(delta.x(), delta.y())
            self._last_middle_pos = current
            self._mark_interaction()
            self.middleDragged.emit(event)
            self.request_redraw(DirtyLayer.VIEW)
            event.accept()
            return

        if event.buttons() & Qt.LeftButton:
            x, y = self.screen_to_world(event.position())
            self._pending_drag = (_PointerEventSnapshot(event), x, y)
            elapsed_ms = (time.monotonic() - self._last_drag_emit) * 1000.0
            if elapsed_ms >= self._frame_interval_ms:
                self._emit_pending_drag()
            elif not self._drag_timer.isActive():
                self._drag_timer.start(max(1, int(self._frame_interval_ms - elapsed_ms)))
            self._mark_interaction()
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
        self._mark_interaction()
        self.request_redraw(DirtyLayer.VIEW)
        event.accept()

    # ------------------------------------------------------------------
    # Layer painting
    # ------------------------------------------------------------------
    def _paint_layer(self, painter: QPainter, layer_mask: int) -> None:
        painter.setRenderHint(QPainter.Antialiasing, False)
        painter.setRenderHint(QPainter.TextAntialiasing, not self._fast_interaction)

        if layer_mask == DirtyLayer.DXF:
            painter.fillRect(QRectF(0, 0, self.width(), self.height()), self._background)
            if self.show_dxf:
                self._draw_dxf(painter)
            return
        if layer_mask == DirtyLayer.EDGES:
            if self.show_graph:
                self._draw_edges(painter)
                if self.show_network and self.show_network_links:
                    self._draw_network_links(painter)
            return
        if layer_mask == DirtyLayer.OBJECTS:
            if self.show_graph:
                self._draw_departments(painter)
                self._draw_points(painter)
                if self.show_network and self.show_network_assets:
                    self._draw_network_assets(painter)
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
        if self.show_labels and self._scale >= 6.0 and not self._fast_interaction:
            self._draw_dxf_text(painter, visible_tiles)

    def _draw_dxf_text(self, painter: QPainter, tiles: Sequence[_DxfTile]) -> None:
        count = 0
        for tile in tiles:
            for entity in tile.text_entities:
                if count >= self._max_dxf_labels:
                    return
                text = str(entity.get("text") or "").strip()
                if not text:
                    continue
                x, y = entity.get("insert", (0.0, 0.0))
                screen = self.world_to_screen(float(x), float(y))
                if not self.rect().adjusted(-120, -120, 120, 120).contains(screen.toPoint()):
                    continue
                raw_height = float(entity.get("height") or 0.0)
                world_height = max(0.45, min(3.0, raw_height if raw_height > 0 else 0.8))
                pixel_size = max(6, min(64, int(round(world_height * self._scale))))
                self._draw_cached_text(
                    painter,
                    screen,
                    text,
                    QColor("#C0C0C0"),
                    pixel_size,
                    rotation=-float(entity.get("rotation", 0.0)),
                )
                count += 1

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
                labels.append((self.world_to_screen(float(dept.get("x", 0.0)), float(dept.get("y", 0.0))), str(dept.get("name") or department_id), QColor("#aaf7ea")))
        painter.restore()
        self._draw_label_batch(painter, labels)

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
                self._draw_diamond(painter, pos, 0.45, QColor("#b07cff"), QColor("#ffffff") if selected else QColor("#d5bbff"))
                label_color = QColor("#eadcff")
            else:
                self._draw_diamond(painter, pos, 0.5, QColor("#ff7b72"), QColor("#ffffff") if selected else QColor("#ffb3ae"))
                label_color = QColor("#ffb3ae")
            if self.show_labels:
                labels.append((self.world_to_screen(float(point.get("x", 0.0)), float(point.get("y", 0.0))), str(name), label_color))
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
        if asset_type in {"network_router", "firewall", "network_switch", "wireless_access_point", "optical_line_terminal", "optical_network_terminal"}:
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
                labels.append((self.world_to_screen(float(instance.get("x", 0.0)), float(instance.get("y", 0.0))), str(instance.get("name") or instance_id), QColor("#cce7ff")))
        painter.restore()
        self._draw_label_batch(painter, labels)

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

    def _draw_label_batch(self, painter: QPainter, labels: Sequence[Tuple[QPointF, str, QColor]]) -> None:
        if not self.show_labels or self._scale < 2.5:
            return
        limit = min(self._max_graph_labels, 250 if self._fast_interaction else self._max_graph_labels)
        pixel_size = max(6, min(48, int(round(0.25 * self._scale))))
        for index, (screen, text, color) in enumerate(labels):
            if index >= limit:
                break
            if not self.rect().adjusted(-120, -120, 120, 120).contains(screen.toPoint()):
                continue
            self._draw_cached_text(
                painter,
                QPointF(screen.x() + 5.0, screen.y() - 5.0),
                text,
                color,
                pixel_size,
            )

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
