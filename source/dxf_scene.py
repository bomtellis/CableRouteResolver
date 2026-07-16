import math
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from PySide6.QtCore import QPointF, QRectF, Qt
from PySide6.QtGui import QColor, QPainterPath, QPen, QBrush
from PySide6.QtWidgets import (
    QGraphicsItem,
    QGraphicsPathItem,
    QGraphicsScene,
    QGraphicsSimpleTextItem,
)

try:
    import ezdxf
except Exception:  # pragma: no cover
    ezdxf = None


class _EntitySpatialIndex:
    """Uniform-grid index for fast viewport entity queries."""

    def __init__(self, entities: List[Dict], bounds):
        self.entities = entities
        self.cells = defaultdict(list)
        self.global_indices = []
        self.origin_x = 0.0
        self.origin_y = 0.0
        self.cell_size = 25.0
        if bounds and len(bounds) == 4:
            min_x, min_y, max_x, max_y = [float(v) for v in bounds]
            self.origin_x = min_x
            self.origin_y = min_y
            span = max(1.0, max(max_x - min_x, max_y - min_y))
            target = max(8, min(128, int(math.sqrt(max(1, len(entities))))))
            self.cell_size = max(1.0, span / float(target))
        self._build()

    def _cell_range(self, bbox):
        if not bbox or len(bbox) != 4:
            return None
        x1, y1, x2, y2 = [float(v) for v in bbox]
        ix1 = math.floor((min(x1, x2) - self.origin_x) / self.cell_size)
        ix2 = math.floor((max(x1, x2) - self.origin_x) / self.cell_size)
        iy1 = math.floor((min(y1, y2) - self.origin_y) / self.cell_size)
        iy2 = math.floor((max(y1, y2) - self.origin_y) / self.cell_size)
        return ix1, iy1, ix2, iy2

    def _build(self):
        for index, entity in enumerate(self.entities):
            cell_range = self._cell_range(entity.get("bbox"))
            if cell_range is None:
                self.global_indices.append(index)
                continue
            ix1, iy1, ix2, iy2 = cell_range
            cell_count = (ix2 - ix1 + 1) * (iy2 - iy1 + 1)
            if cell_count > 64:
                self.global_indices.append(index)
                continue
            for ix in range(ix1, ix2 + 1):
                for iy in range(iy1, iy2 + 1):
                    self.cells[(ix, iy)].append(index)

    def query(self, bounds) -> Iterable[Dict]:
        cell_range = self._cell_range(bounds)
        if cell_range is None:
            return tuple(self.entities)
        ix1, iy1, ix2, iy2 = cell_range
        seen = set(self.global_indices)
        for ix in range(ix1, ix2 + 1):
            for iy in range(iy1, iy2 + 1):
                seen.update(self.cells.get((ix, iy), ()))
        return tuple(self.entities[index] for index in sorted(seen))


class DXFScene:
    def __init__(self):
        self.path = None
        self.entities: List[Dict] = []
        self.bounds: Optional[Tuple[float, float, float, float]] = None
        self.revision = 0
        self._spatial_index = None

    def clear(self):
        self.path = None
        self.entities = []
        self.bounds = None
        self.revision += 1
        self._spatial_index = None

    def set_content(
        self,
        path: Optional[str],
        entities: Optional[List[Dict]],
        bounds: Optional[Tuple[float, float, float, float]],
    ):
        self.path = path
        self.entities = list(entities or [])
        self.bounds = bounds
        self.revision += 1
        self._spatial_index = None

    @classmethod
    def from_content(
        cls,
        path: Optional[str],
        entities: Optional[List[Dict]],
        bounds: Optional[Tuple[float, float, float, float]],
    ):
        scene = cls()
        scene.set_content(path, entities, bounds)
        return scene

    @staticmethod
    def _bbox_from_points(points):
        if not points:
            return None
        xs = [float(p[0]) for p in points]
        ys = [float(p[1]) for p in points]
        return (min(xs), min(ys), max(xs), max(ys))
    
    @staticmethod
    def _bbox_to_scene_rect(bbox, padding: float = 0.0) -> Optional[QRectF]:
        if not bbox or len(bbox) != 4:
            return None

        min_x, min_y, max_x, max_y = [float(v) for v in bbox]

        return QRectF(
            min_x - padding,
            -(max_y + padding),
            max(0.001, (max_x - min_x) + (padding * 2.0)),
            max(0.001, (max_y - min_y) + (padding * 2.0)),
        )


    def query_entities_world(
        self,
        min_x: float,
        min_y: float,
        max_x: float,
        max_y: float,
        padding: float = 0.0,
    ) -> Iterable[Dict]:
        """Return only entities that can intersect a world-coordinate viewport."""
        bounds = (
            float(min_x) - float(padding),
            float(min_y) - float(padding),
            float(max_x) + float(padding),
            float(max_y) + float(padding),
        )
        if self._spatial_index is None:
            self._spatial_index = _EntitySpatialIndex(self.entities, self.bounds)
        return self._spatial_index.query(bounds)

    @classmethod
    def _bbox_intersects_scene_rect(
        cls,
        bbox,
        visible_rect: Optional[QRectF],
        padding: float = 0.0,
    ) -> bool:
        if visible_rect is None:
            return True

        entity_rect = cls._bbox_to_scene_rect(bbox, padding=padding)

        if entity_rect is None:
            return True

        return visible_rect.intersects(entity_rect)

    @classmethod
    def load_content(cls, path: str):
        if ezdxf is None:
            raise RuntimeError(
                "ezdxf is not installed. Install with: pip install ezdxf"
            )

        doc = ezdxf.readfile(path)
        msp = doc.modelspace()
        entities: List[Dict] = []
        all_points = []

        # CableRouteResolver stores all project geometry in metres.  DXF model
        # space coordinates are expressed in the unit declared by $INSUNITS;
        # keeping those raw values made a millimetre drawing 1000 times larger
        # than the routing graph and prevented otherwise identical origins from
        # lining up.
        unit_scale = 1.0
        try:
            from ezdxf import units

            drawing_units = int(getattr(doc, "units", 0) or 0)
            if drawing_units:
                unit_scale = float(units.conversion_factor(drawing_units, units.M))
        except (TypeError, ValueError, IndexError):
            # Unitless and uncommon unsupported unit declarations retain the
            # historical 1 drawing unit == 1 metre behaviour.
            unit_scale = 1.0

        def metres_point(point):
            return float(point[0]) * unit_scale, float(point[1]) * unit_scale

        def append_entity(entity: Dict):
            if "bbox" not in entity or entity["bbox"] is None:
                entity["bbox"] = cls._bbox_from_points(entity.get("points", []))
            entities.append(entity)

        def track_points(points):
            for x, y in points:
                all_points.append((float(x), float(y)))

        def add_line(start, end):
            points = [
                metres_point(start),
                metres_point(end),
            ]
            track_points(points)
            append_entity(
                {
                    "type": "LINE",
                    "start": points[0],
                    "end": points[1],
                    "bbox": cls._bbox_from_points(points),
                }
            )

        def add_polyline(points, closed=False):
            if len(points) < 2:
                return
            clean = [metres_point((x, y)) for x, y in points]
            track_points(clean)
            append_entity(
                {
                    "type": "POLYLINE",
                    "points": clean,
                    "closed": bool(closed),
                    "bbox": cls._bbox_from_points(clean),
                }
            )

        def add_text_entity(insert, text, height=2.5, rotation=0.0):
            x, y = metres_point(insert)
            h = float(height or 2.5) * unit_scale
            track_points([(x, y), (x + h, y + h)])
            append_entity(
                {
                    "type": "TEXT",
                    "insert": (x, y),
                    "text": str(text),
                    "height": h,
                    "rotation": float(rotation or 0.0),
                    "bbox": (x, y - h, x + max(h, len(str(text)) * h * 0.6), y + h),
                }
            )

        def add_circle(center, radius):
            cx, cy = metres_point(center)
            r = float(radius) * unit_scale
            bbox = (cx - r, cy - r, cx + r, cy + r)
            track_points([(bbox[0], bbox[1]), (bbox[2], bbox[3])])
            append_entity(
                {"type": "CIRCLE", "center": (cx, cy), "radius": r, "bbox": bbox}
            )

        def add_arc(center, radius, start_angle, end_angle):
            cx, cy = metres_point(center)
            r = float(radius) * unit_scale
            bbox = (cx - r, cy - r, cx + r, cy + r)
            track_points([(bbox[0], bbox[1]), (bbox[2], bbox[3])])
            append_entity(
                {
                    "type": "ARC",
                    "center": (cx, cy),
                    "radius": r,
                    "start_angle": float(start_angle),
                    "end_angle": float(end_angle),
                    "bbox": bbox,
                }
            )

        def load_hatch(entity):
            try:
                boundary_paths = entity.paths
            except Exception:
                return

            for path in boundary_paths:
                points = []
                try:
                    if hasattr(path, "vertices"):
                        for vx in path.vertices:
                            points.append((float(vx[0]), float(vx[1])))
                    elif hasattr(path, "edges"):
                        for edge in path.edges:
                            edge_type = edge.__class__.__name__
                            if edge_type == "LineEdge":
                                points.append(
                                    (float(edge.start[0]), float(edge.start[1]))
                                )
                                points.append((float(edge.end[0]), float(edge.end[1])))
                            elif edge_type == "ArcEdge":
                                cx = float(edge.center[0])
                                cy = float(edge.center[1])
                                r = float(edge.radius)
                                start = math.radians(float(edge.start_angle))
                                end = math.radians(float(edge.end_angle))
                                if end < start:
                                    end += math.tau
                                steps = 24
                                for i in range(steps + 1):
                                    a = start + ((end - start) * i / steps)
                                    points.append(
                                        (cx + (r * math.cos(a)), cy + (r * math.sin(a)))
                                    )
                    if points:
                        add_polyline(points, closed=True)
                except Exception:
                    continue

        def load_insert(entity, doc_ref):
            try:
                block = doc_ref.blocks.get(entity.dxf.name)
            except Exception:
                return

            insert = entity.dxf.insert
            ix = float(insert.x)
            iy = float(insert.y)
            try:
                base = block.block.dxf.base_point
                bx = float(base.x)
                by = float(base.y)
            except Exception:
                bx = 0.0
                by = 0.0
            sx = float(getattr(entity.dxf, "xscale", 1.0) or 1.0)
            sy = float(getattr(entity.dxf, "yscale", 1.0) or 1.0)
            rotation = math.radians(float(getattr(entity.dxf, "rotation", 0.0) or 0.0))
            cos_r = math.cos(rotation)
            sin_r = math.sin(rotation)

            def transform_point(x, y):
                # INSERT positions refer to the block base point, not (0, 0).
                # Omitting this subtraction offsets every block whose author
                # chose a non-zero base point.
                x = (float(x) - bx) * sx
                y = (float(y) - by) * sy
                rx = (x * cos_r) - (y * sin_r)
                ry = (x * sin_r) + (y * cos_r)
                return ix + rx, iy + ry

            for child in block:
                try:
                    dtype = child.dxftype()
                    if dtype == "LINE":
                        s = child.dxf.start
                        e = child.dxf.end
                        add_line(transform_point(s.x, s.y), transform_point(e.x, e.y))
                    elif dtype in {"LWPOLYLINE", "POLYLINE"}:
                        points = []
                        try:
                            raw_points = list(child.get_points())
                            for p in raw_points:
                                points.append(transform_point(float(p[0]), float(p[1])))
                        except Exception:
                            try:
                                for v in child.vertices:
                                    points.append(
                                        transform_point(
                                            float(v.dxf.location.x),
                                            float(v.dxf.location.y),
                                        )
                                    )
                            except Exception:
                                continue
                        add_polyline(
                            points, closed=bool(getattr(child, "closed", False))
                        )
                    elif dtype == "TEXT":
                        p = child.dxf.insert
                        tx, ty = transform_point(p.x, p.y)
                        add_text_entity(
                            (tx, ty),
                            child.dxf.text,
                            child.dxf.height,
                            float(getattr(child.dxf, "rotation", 0.0) or 0.0),
                        )
                    elif dtype == "MTEXT":
                        p = child.dxf.insert
                        tx, ty = transform_point(p.x, p.y)
                        add_text_entity(
                            (tx, ty),
                            child.text,
                            child.dxf.char_height,
                            float(getattr(child.dxf, "rotation", 0.0) or 0.0),
                        )
                except Exception:
                    continue

        for entity in msp:
            dtype = entity.dxftype()
            if dtype == "LINE":
                start = entity.dxf.start
                end = entity.dxf.end
                add_line((start.x, start.y), (end.x, end.y))
            elif dtype in {"LWPOLYLINE", "POLYLINE"}:
                points = []
                try:
                    raw_points = list(entity.get_points())
                    for p in raw_points:
                        points.append((float(p[0]), float(p[1])))
                except Exception:
                    try:
                        for v in entity.vertices:
                            points.append(
                                (float(v.dxf.location.x), float(v.dxf.location.y))
                            )
                    except Exception:
                        continue
                add_polyline(points, closed=bool(getattr(entity, "closed", False)))
            elif dtype == "CIRCLE":
                center = entity.dxf.center
                add_circle((center.x, center.y), entity.dxf.radius)
            elif dtype == "ARC":
                center = entity.dxf.center
                add_arc(
                    (center.x, center.y),
                    entity.dxf.radius,
                    entity.dxf.start_angle,
                    entity.dxf.end_angle,
                )
            elif dtype == "TEXT":
                insert = entity.dxf.insert
                add_text_entity(
                    (insert.x, insert.y),
                    entity.dxf.text,
                    entity.dxf.height,
                    getattr(entity.dxf, "rotation", 0.0),
                )
            elif dtype == "MTEXT":
                insert = entity.dxf.insert
                add_text_entity(
                    (insert.x, insert.y),
                    entity.text,
                    entity.dxf.char_height,
                    getattr(entity.dxf, "rotation", 0.0),
                )
            elif dtype == "HATCH":
                load_hatch(entity)
            elif dtype == "INSERT":
                load_insert(entity, doc)

        bounds = (
            cls._bbox_from_points(all_points)
            if all_points
            else (0.0, 0.0, 100.0, 100.0)
        )
        return {"path": path, "entities": entities, "bounds": bounds}

    def load(self, path: str):
        payload = self.load_content(path)
        self.set_content(payload["path"], payload["entities"], payload["bounds"])

    def scene_rect(self, padding: float = 40.0) -> QRectF:
        if not self.bounds:
            return QRectF(-padding, -padding, 100 + 2 * padding, 100 + 2 * padding)
        min_x, min_y, max_x, max_y = self.bounds
        return QRectF(
            min_x - padding,
            -(max_y + padding),
            (max_x - min_x) + 2 * padding,
            (max_y - min_y) + 2 * padding,
        )

    def populate_graphics_scene(
        self,
        scene: QGraphicsScene,
        view_scale: float = 1.0,
        visible_rect: Optional[QRectF] = None,
        cull_padding: float = 25.0,
    ):
        pen_line = QPen(QColor("#858585"))
        pen_line.setWidthF(0.0)

        pen_poly = QPen(QColor("#bebebe"))
        pen_poly.setWidthF(0.0)

        pen_arc = QPen(QColor("#2e2e2e"))
        pen_arc.setWidthF(0.0)

        line_path = QPainterPath()
        poly_path = QPainterPath()
        arc_path = QPainterPath()
        text_items = []
        created_items = []

        if visible_rect is None:
            candidate_entities = self.entities
        else:
            # Convert scene coordinates (Y down) back to the DXF world coordinates.
            min_x = float(visible_rect.left())
            max_x = float(visible_rect.right())
            min_y = -float(visible_rect.bottom())
            max_y = -float(visible_rect.top())
            candidate_entities = self.query_entities_world(
                min_x, min_y, max_x, max_y, padding=cull_padding
            )

        for entity in candidate_entities:
            if not self._bbox_intersects_scene_rect(
                entity.get("bbox"),
                visible_rect,
                padding=cull_padding,
            ):
                continue

            etype = entity["type"]

            if etype == "LINE":
                x1, y1 = entity["start"]
                x2, y2 = entity["end"]
                line_path.moveTo(x1, -y1)
                line_path.lineTo(x2, -y2)

            elif etype == "POLYLINE":
                pts = [QPointF(x, -y) for x, y in entity["points"]]
                if len(pts) >= 2:
                    poly_path.moveTo(pts[0])
                    for pt in pts[1:]:
                        poly_path.lineTo(pt)
                    if entity.get("closed"):
                        poly_path.closeSubpath()

            elif etype == "CIRCLE":
                cx, cy = entity["center"]
                r = float(entity["radius"])
                arc_path.addEllipse(QRectF(cx - r, -(cy + r), r * 2, r * 2))

            elif etype == "ARC":
                cx, cy = entity["center"]
                r = float(entity["radius"])
                start_angle = float(entity.get("start_angle", 0.0))
                end_angle = float(entity.get("end_angle", 0.0))
                span_angle = end_angle - start_angle
                if span_angle <= 0:
                    span_angle += 360.0
                rect = QRectF(cx - r, -(cy + r), r * 2, r * 2)
                arc_path.arcMoveTo(rect, -start_angle)
                arc_path.arcTo(rect, -start_angle, -span_angle)

            elif etype == "TEXT":
                text = (entity.get("text") or "").strip()
                if not text:
                    continue
                if view_scale < 6:
                    continue

                text_height = float(entity.get("height") or 0.0)
                if text_height > 40.0:
                    continue

                x, y = entity["insert"]
                item = QGraphicsSimpleTextItem(text)
                item.setBrush(QBrush(QColor("#C0C0C0")))
                font = item.font()

                if text_height < 1:
                    font_size = 12
                    if view_scale <= 8:
                        font_size = 6
                    elif view_scale >= 22:
                        font_size = 12
                    else:
                        font_size = 6 * math.pow(2, (view_scale - 8) / 9)
                else:
                    font_size = 24

                font.setPixelSize(int(font_size))
                item.setFont(font)
                item.setFlag(QGraphicsItem.ItemIgnoresTransformations, True)
                item.setPos(x, -y)
                item.setRotation(-float(entity.get("rotation", 0.0)))
                item.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
                text_items.append(item)

        if not line_path.isEmpty():
            item = QGraphicsPathItem(line_path)
            item.setPen(pen_line)
            item.setBrush(Qt.NoBrush)
            item.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
            scene.addItem(item)
            created_items.append(item)

        if not poly_path.isEmpty():
            item = QGraphicsPathItem(poly_path)
            item.setPen(pen_poly)
            item.setBrush(Qt.NoBrush)
            item.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
            scene.addItem(item)
            created_items.append(item)

        if not arc_path.isEmpty():
            item = QGraphicsPathItem(arc_path)
            item.setPen(pen_arc)
            item.setBrush(Qt.NoBrush)
            item.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
            scene.addItem(item)
            created_items.append(item)

        for item in text_items:
            scene.addItem(item)
            created_items.append(item)

        return created_items
