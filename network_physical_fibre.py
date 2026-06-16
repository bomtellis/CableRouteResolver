"""Editable physical-fibre floor topology, separate from the logical network view."""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

from PySide6.QtCore import QPoint, QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QBrush, QColor, QFont, QFontMetrics, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QComboBox, QDialog, QFrame, QGraphicsItem, QGraphicsObject, QGraphicsPathItem,
    QGraphicsScene, QGraphicsSimpleTextItem, QGraphicsView, QHBoxLayout, QLabel,
    QMenu, QMessageBox, QPushButton, QToolButton, QVBoxLayout, QWidget,
)

from network_fibre_dialogs import FibreCableEditorDialog, FibreNodeEditorDialog, FibreSpliceEditorDialog
from network_schema import ensure_network_schema, next_network_id
from network_services import cable_core_statistics, cable_route_points, circuit_trace, set_core_status_from_splices, splice_arrangement_rows


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _int(value, default: int = 0) -> int:
    try: return int(value)
    except (TypeError, ValueError): return default


def _float(value, default: float = 0.0) -> float:
    try: return float(value)
    except (TypeError, ValueError): return default


class FibreMapView(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setRenderHints(QPainter.Antialiasing | QPainter.TextAntialiasing)
        self.setFrameShape(QFrame.NoFrame)
        self.setBackgroundBrush(QColor("#10161c"))
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.AnchorViewCenter)

    def wheelEvent(self, event):
        delta = event.angleDelta().y()
        if not delta: return super().wheelEvent(event)
        factor = 1.18 if delta > 0 else 1 / 1.18
        scale = abs(self.transform().m11())
        if 0.02 <= scale * factor <= 50:
            self.scale(factor, factor)
        event.accept()

    def fit_content(self):
        if not self.scene(): return
        rect = self.scene().itemsBoundingRect().adjusted(-20, -20, 20, 20)
        if not rect.isEmpty():
            self.resetTransform(); self.fitInView(rect, Qt.KeepAspectRatio)


class FibreNodeItem(QGraphicsObject):
    contextRequested = Signal(str, object)
    moved = Signal(str, float, float)
    activated = Signal(str)

    def __init__(self, node: dict, traced: bool = False, symbol_scale: float = 0.32):
        super().__init__(); self.node = node; self.traced = traced
        self.symbol_scale = max(0.08, float(symbol_scale or 0.32))
        self.setFlags(QGraphicsItem.ItemIsSelectable | QGraphicsItem.ItemIsMovable | QGraphicsItem.ItemSendsGeometryChanges)
        self.setAcceptHoverEvents(True); self.setToolTip(self._tooltip())

    def boundingRect(self):
        size = 26.0 * self.symbol_scale
        return QRectF(-size / 2.0, -size / 2.0, size, size)

    def _tooltip(self):
        return f"{_text(self.node.get('name'))}\n{_text(self.node.get('node_type')).replace('_',' ').title()}\n{_text(self.node.get('location_name'))} · Floor {_int(self.node.get('floor'))}"

    def paint(self, painter, option, widget=None):
        painter.save()
        painter.scale(self.symbol_scale, self.symbol_scale)
        painter.setRenderHint(QPainter.Antialiasing, True)
        node_type = _text(self.node.get("node_type"))
        colour = {
            "splice_enclosure": QColor("#9167c6"), "splice_cassette": QColor("#b77bdc"),
            "fibre_joint": QColor("#e19b52"), "termination": QColor("#5f9ee3"),
            "handhole": QColor("#77838d"), "chamber": QColor("#53616d"),
        }.get(node_type, QColor("#77838d"))
        border = QColor("#ff8a24") if self.traced else (QColor("#8fc7ff") if self.isSelected() else QColor("#d5dce1"))
        painter.setPen(QPen(border, 2.8 if self.traced or self.isSelected() else 1.2)); painter.setBrush(colour)
        base_rect = QRectF(-13, -13, 26, 26)
        if node_type in {"splice_enclosure", "fibre_joint"}:
            painter.drawEllipse(base_rect.adjusted(2,2,-2,-2))
        elif node_type == "splice_cassette":
            painter.drawRoundedRect(base_rect.adjusted(2,4,-2,-4), 3, 3)
            painter.drawLine(QPointF(-8,-2), QPointF(8,-2)); painter.drawLine(QPointF(-8,3), QPointF(8,3))
        elif node_type in {"handhole", "chamber"}:
            painter.drawRect(base_rect.adjusted(2,2,-2,-2))
        else:
            painter.drawRoundedRect(base_rect.adjusted(2,2,-2,-2), 5, 5)
        painter.setPen(QColor("#ffffff")); painter.setFont(QFont("Arial", 6, QFont.Bold))
        initials = {"splice_enclosure":"SE","splice_cassette":"SC","fibre_joint":"J","termination":"T","handhole":"HH","chamber":"CH"}.get(node_type,"F")
        painter.drawText(base_rect, Qt.AlignCenter, initials)
        painter.restore()

    def mouseDoubleClickEvent(self, event): self.activated.emit(_text(self.node.get("id"))); event.accept()
    def contextMenuEvent(self, event): self.contextRequested.emit(_text(self.node.get("id")), event.screenPos()); event.accept()
    def mouseReleaseEvent(self, event):
        super().mouseReleaseEvent(event)
        pos = self.pos(); self.moved.emit(_text(self.node.get("id")), pos.x(), -pos.y())


class FibreCableItem(QGraphicsPathItem):
    def __init__(self, cable: dict, path: QPainterPath, traced: bool, context_callback: Callable[[str, QPoint], None], width_scale: float = 0.55):
        super().__init__(path); self.cable = cable; self.traced = traced; self.context_callback = context_callback
        self.width_scale = max(0.2, float(width_scale or 0.55))
        self.setFlags(QGraphicsItem.ItemIsSelectable); self.setAcceptHoverEvents(True); self.setZValue(-0.2)
        self._update_pen(False)
        stats = cable_core_statistics(cable)
        self.setToolTip(f"{_text(cable.get('name'))}\n{stats['used']}/{stats['total']} cores used · {stats['dark']} dark\n{_float(cable.get('length_m')):.1f} m")

    def _update_pen(self, hover: bool):
        colour = QColor("#ff8a24") if self.traced else QColor("#6f8dff")
        width = (5.0 if self.traced else (4.0 if hover or self.isSelected() else 2.6)) * self.width_scale
        self.setPen(QPen(colour, width, Qt.SolidLine, Qt.RoundCap, Qt.RoundJoin))
    def hoverEnterEvent(self,event): self._update_pen(True); super().hoverEnterEvent(event)
    def hoverLeaveEvent(self,event): self._update_pen(False); super().hoverLeaveEvent(event)
    def contextMenuEvent(self,event): self.context_callback(_text(self.cable.get("id")), event.screenPos()); event.accept()


class PhysicalFibreTopologyDialog(QDialog):
    """Editable floor map for physical fibre, splices and terminations."""
    def __init__(self, parent, data: dict, on_change: Optional[Callable[[dict], None]] = None, initial_trace_connection_id: str = ""):
        super().__init__(parent); self.data = data; ensure_network_schema(self.data); self.on_change = on_change
        self.trace = circuit_trace(self.data, initial_trace_connection_id) if initial_trace_connection_id else {}
        self.setWindowTitle("Physical Fibre Topology"); self.setWindowFlag(Qt.Window, True); self.resize(1500, 900)
        self.setStyleSheet("QDialog{background:#10161c;color:#e8edf1} QPushButton,QComboBox{background:#25303b;color:#e8edf1;border:1px solid #46535e;padding:5px;border-radius:4px} QLabel{color:#d8e0e6}")
        layout = QVBoxLayout(self); toolbar = QHBoxLayout(); layout.addLayout(toolbar)
        title = QLabel("Physical fibre overlay"); title.setFont(QFont("Arial", 13, QFont.Bold)); toolbar.addWidget(title)
        self.floor_combo = QComboBox(); toolbar.addWidget(self.floor_combo)
        for label, handler in (("Add enclosure / cassette", self.add_node), ("Add fibre cable", self.add_cable), ("Add splice", self.add_splice), ("Clear trace", self.clear_trace), ("Fit", self._fit), ("Refresh", self.refresh)):
            button = QPushButton(label); button.clicked.connect(handler); toolbar.addWidget(button)
        toolbar.addStretch(1)
        self.layer_label = QLabel(); toolbar.addWidget(self.layer_label)
        self.scene = QGraphicsScene(self); self.view = FibreMapView(self); self.view.setScene(self.scene); layout.addWidget(self.view, 1)
        self.status = QLabel(); self.status.setStyleSheet("padding:7px;background:#182028;color:#aeb9c2"); layout.addWidget(self.status)
        self.floor_combo.currentIndexChanged.connect(self.rebuild)
        self._populate_floors(); self.rebuild()

    def _populate_floors(self):
        current = self.floor_combo.currentData(); self.floor_combo.blockSignals(True); self.floor_combo.clear()
        floors = set()
        for collection in (self.data.get("locations", []), self.data.get("corridors", {}).get("nodes", []), self.data.get("network_fibre_nodes", [])):
            floors.update(_int(row.get("floor")) for row in collection if isinstance(row, dict))
        for floor in sorted(floors or {0}): self.floor_combo.addItem(f"Floor {floor}", floor)
        idx=self.floor_combo.findData(current); self.floor_combo.setCurrentIndex(idx if idx>=0 else 0); self.floor_combo.blockSignals(False)

    def _floor(self): return _int(self.floor_combo.currentData())
    def _layer_settings(self): return self.data.get("network_settings",{}).get("physical_fibre_layer",{})
    def _commit(self, label="Update physical fibre"):
        ensure_network_schema(self.data)
        if self.on_change: self.on_change(deepcopy(self.data))
        self.rebuild()

    def _all_route_points(self):
        points={}
        for row in self.data.get("corridors",{}).get("nodes",[]):
            if isinstance(row,dict) and _text(row.get("name")): points[_text(row.get("name"))]=row
        for row in self.data.get("locations",[]):
            if isinstance(row,dict) and _text(row.get("name")): points[_text(row.get("name"))]=row
        return points

    def rebuild(self, *_args):
        ensure_network_schema(self.data); self.scene.clear(); floor=self._floor(); layer=self._layer_settings(); self.layer_label.setText(f"DXF layers: {layer.get('cable_layer','NET-FIBRE-CABLE')} / {layer.get('splice_layer','NET-FIBRE-SPLICE')}")
        symbol_scale=max(0.08,_float(layer.get("symbol_scale"),0.32)); label_scale=max(0.2,_float(layer.get("label_scale"),0.55)); width_scale=max(0.2,_float(layer.get("cable_width_scale"),0.55))
        if layer.get("show_base_graph", True): self._draw_base_graph(floor)
        traced_cables=set(self.trace.get("fibre_cable_ids",[])); traced_nodes=set()
        for splice in self.data.get("network_fibre_splices",[]):
            if _text(splice.get("id")) in set(self.trace.get("splice_ids",[])): traced_nodes.add(_text(splice.get("node_id")))
        cable_count=0
        for cable in self.data.get("network_fibre_cables",[]):
            if not isinstance(cable,dict): continue
            points=[p for p in cable_route_points(self.data,cable) if _int(p.get("floor"))==floor]
            if len(points)<2: continue
            path=QPainterPath(QPointF(_float(points[0].get("x")),-_float(points[0].get("y"))))
            for point in points[1:]: path.lineTo(QPointF(_float(point.get("x")),-_float(point.get("y"))))
            item=FibreCableItem(cable,path,_text(cable.get("id")) in traced_cables,self._cable_context,width_scale); self.scene.addItem(item); cable_count+=1
            midpoint=path.pointAtPercent(0.5); stats=cable_core_statistics(cable)
            label_text=f"{_text(cable.get('id'))} · {_int(cable.get('core_count'))}F"
            if layer.get("show_dark_fibre",True): label_text+=f" · {stats['dark']} dark"
            label=QGraphicsSimpleTextItem(label_text); label.setBrush(QBrush(QColor("#ffb25f") if item.traced else QColor("#b8c8ff"))); label.setFont(QFont("Arial",max(1,int(round(7*label_scale))),QFont.Bold)); label.setPos(midpoint+QPointF(5*label_scale,-18*label_scale)); label.setZValue(0.3); self.scene.addItem(label)
        node_count=0
        for node in self.data.get("network_fibre_nodes",[]):
            if not isinstance(node,dict) or _int(node.get("floor"))!=floor: continue
            item=FibreNodeItem(node,_text(node.get("id")) in traced_nodes,symbol_scale); item.setPos(_float(node.get("x")),-_float(node.get("y"))); item.contextRequested.connect(self._node_context); item.moved.connect(self._node_moved); item.activated.connect(self.edit_node); self.scene.addItem(item); node_count+=1
            label=QGraphicsSimpleTextItem(_text(node.get("label")) or _text(node.get("name"))); label.setBrush(QBrush(QColor("#dce5ea"))); label.setFont(QFont("Arial",max(1,int(round(7*label_scale))))); label.setPos(item.pos()+QPointF(15*symbol_scale,-10*symbol_scale)); self.scene.addItem(label)
            if layer.get("show_splice_labels",True):
                rows=splice_arrangement_rows(self.data,_text(node.get("id")))
                if rows:
                    splice_label=QGraphicsSimpleTextItem(f"{len(rows)} splice{'s' if len(rows)!=1 else ''}"); splice_label.setBrush(QBrush(QColor("#d5a5f1"))); splice_label.setFont(QFont("Arial",max(1,int(round(6*label_scale))))); splice_label.setPos(item.pos()+QPointF(15*symbol_scale,2*symbol_scale)); self.scene.addItem(splice_label)
        margin=max(20.0,40.0*max(symbol_scale,label_scale)); rect=self.scene.itemsBoundingRect().adjusted(-margin,-margin,margin,margin); self.scene.setSceneRect(rect if not rect.isEmpty() else QRectF(-100,-100,200,200))
        self.status.setText(f"Floor {floor}: {cable_count} routed fibre cables, {node_count} fibre nodes, {len([s for s in self.data.get('network_fibre_splices',[]) if _text(s.get('node_id')) in {_text(n.get('id')) for n in self.data.get('network_fibre_nodes',[]) if _int(n.get('floor'))==floor}])} core splices · Drag fibre nodes to reposition · Right-click cables and nodes to edit or trace")

    def _draw_base_graph(self,floor):
        points=self._all_route_points(); pen=QPen(QColor("#2d3740"),0.8)
        for edge in self.data.get("corridors",{}).get("edges",[]):
            if not isinstance(edge,dict): continue
            a=points.get(_text(edge.get("from"))); b=points.get(_text(edge.get("to")))
            if not a or not b or _int(a.get("floor"))!=floor or _int(b.get("floor"))!=floor: continue
            path=QPainterPath(QPointF(_float(a.get("x")),-_float(a.get("y")))); path.lineTo(QPointF(_float(b.get("x")),-_float(b.get("y"))))
            item=QGraphicsPathItem(path); item.setPen(pen); item.setZValue(-2); self.scene.addItem(item)

    def _fit(self): self.view.fit_content()
    def refresh(self): self._populate_floors(); self.rebuild()
    def clear_trace(self): self.trace={}; self.rebuild()
    def trace_cable(self,cable_id):
        cable=next((c for c in self.data.get("network_fibre_cables",[]) if _text(c.get("id"))==_text(cable_id)),None)
        if cable:
            logical=next((_text(v) for v in cable.get("logical_connection_ids",[]) if _text(v)),"")
            self.trace=circuit_trace(self.data,logical) if logical else {"fibre_cable_ids":[cable_id],"splice_ids":list(cable.get("splice_ids",[]))}
            self.rebuild()

    def add_node(self):
        dialog=FibreNodeEditorDialog(self,nodes=self.data.get("network_fibre_nodes",[]),instances=self.data.get("network_asset_instances",[]),locations=self.data.get("locations",[]),suggested_id=next_network_id(self.data.get("network_fibre_nodes",[]),"FN"),default_floor=self._floor())
        if dialog.exec()==QDialog.Accepted and dialog.result: self.data.setdefault("network_fibre_nodes",[]).append(dialog.result); self._commit()
    def edit_node(self,node_id):
        nodes=self.data.get("network_fibre_nodes",[]); index=next((i for i,n in enumerate(nodes) if _text(n.get("id"))==_text(node_id)),-1)
        if index<0:return
        dialog=FibreNodeEditorDialog(self,nodes[index],nodes,self.data.get("network_asset_instances",[]),self.data.get("locations",[]),node_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: nodes[index]=dialog.result; self._commit()
    def delete_node(self,node_id):
        if QMessageBox.question(self,"Delete fibre node",f"Delete fibre node {node_id} and its splice records?")!=QMessageBox.Yes:return
        self.data["network_fibre_nodes"]=[n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))!=node_id and _text(n.get("parent_node_id"))!=node_id]
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("node_id"))!=node_id and _text(s.get("cassette_id"))!=node_id]; self._commit()
    def _node_moved(self,node_id,x,y):
        node=next((n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))==node_id),None)
        if node: node["x"]=round(x,3); node["y"]=round(y,3); node["floor"]=self._floor(); self._commit()

    def add_cable(self):
        dialog=FibreCableEditorDialog(self,instances=self.data.get("network_asset_instances",[]),suggested_id=next_network_id(self.data.get("network_fibre_cables",[]),"FOC"))
        if dialog.exec()==QDialog.Accepted and dialog.result: self.data.setdefault("network_fibre_cables",[]).append(dialog.result); self._commit()
    def edit_cable(self,cable_id):
        cables=self.data.get("network_fibre_cables",[]); index=next((i for i,c in enumerate(cables) if _text(c.get("id"))==cable_id),-1)
        if index<0:return
        dialog=FibreCableEditorDialog(self,cables[index],self.data.get("network_asset_instances",[]),cable_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: cables[index]=dialog.result; self._commit()
    def delete_cable(self,cable_id):
        if QMessageBox.question(self,"Delete fibre cable",f"Delete fibre cable {cable_id} and its splice records?")!=QMessageBox.Yes:return
        self.data["network_fibre_cables"]=[c for c in self.data.get("network_fibre_cables",[]) if _text(c.get("id"))!=cable_id]
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("incoming_cable_id"))!=cable_id and _text(s.get("outgoing_cable_id"))!=cable_id]; self._commit()

    def add_splice(self,node_id="",incoming_cable_id=""):
        seed={"node_id":node_id,"incoming_cable_id":incoming_cable_id}
        dialog=FibreSpliceEditorDialog(self,seed,self.data.get("network_fibre_nodes",[]),self.data.get("network_fibre_cables",[]),next_network_id(self.data.get("network_fibre_splices",[]),"FS"))
        if dialog.exec()==QDialog.Accepted and dialog.result:
            self.data.setdefault("network_fibre_splices",[]).append(dialog.result)
            for cable in self.data.get("network_fibre_cables",[]):
                if _text(cable.get("id")) in {_text(dialog.result.get("incoming_cable_id")),_text(dialog.result.get("outgoing_cable_id"))}:
                    ids=[_text(v) for v in cable.get("splice_ids",[]) if _text(v)]
                    if _text(dialog.result.get("id")) not in ids: ids.append(_text(dialog.result.get("id")))
                    cable["splice_ids"]=ids
            set_core_status_from_splices(self.data); self._commit()
    def edit_splice(self,splice_id):
        splices=self.data.get("network_fibre_splices",[]); index=next((i for i,s in enumerate(splices) if _text(s.get("id"))==splice_id),-1)
        if index<0:return
        dialog=FibreSpliceEditorDialog(self,splices[index],self.data.get("network_fibre_nodes",[]),self.data.get("network_fibre_cables",[]),splice_id)
        if dialog.exec()==QDialog.Accepted and dialog.result: splices[index]=dialog.result; set_core_status_from_splices(self.data); self._commit()
    def delete_splice(self,splice_id):
        if QMessageBox.question(self,"Delete splice",f"Delete splice {splice_id}?")!=QMessageBox.Yes:return
        self.data["network_fibre_splices"]=[s for s in self.data.get("network_fibre_splices",[]) if _text(s.get("id"))!=splice_id]
        for cable in self.data.get("network_fibre_cables",[]): cable["splice_ids"]=[v for v in cable.get("splice_ids",[]) if _text(v)!=splice_id]
        self._commit()

    def _node_context(self,node_id,screen_pos):
        menu=QMenu(self); edit=menu.addAction("Edit fibre node"); cassette=menu.addAction("Add splice cassette inside"); splice=menu.addAction("Add splice at this node"); menu.addSeparator(); delete=menu.addAction("Delete fibre node")
        action=menu.exec(screen_pos)
        if action==edit:self.edit_node(node_id)
        elif action==cassette:
            parent=next((n for n in self.data.get("network_fibre_nodes",[]) if _text(n.get("id"))==node_id),{})
            seed={"node_type":"splice_cassette","parent_node_id":node_id,"location_name":parent.get("location_name", ""),"floor":parent.get("floor",0),"x":parent.get("x",0.0),"y":parent.get("y",0.0)}
            dialog=FibreNodeEditorDialog(self,seed,self.data.get("network_fibre_nodes",[]),self.data.get("network_asset_instances",[]),self.data.get("locations",[]),next_network_id(self.data.get("network_fibre_nodes",[]),"FSC"),self._floor())
            if dialog.exec()==QDialog.Accepted and dialog.result:self.data["network_fibre_nodes"].append(dialog.result);self._commit()
        elif action==splice:self.add_splice(node_id)
        elif action==delete:self.delete_node(node_id)

    def _cable_context(self,cable_id,screen_pos):
        menu=QMenu(self); trace=menu.addAction("Trace circuit"); edit=menu.addAction("Edit fibre cable and cores"); splice=menu.addAction("Add splice using this cable"); menu.addSeparator(); delete=menu.addAction("Delete fibre cable")
        action=menu.exec(screen_pos)
        if action==trace:self.trace_cable(cable_id)
        elif action==edit:self.edit_cable(cable_id)
        elif action==splice:self.add_splice("",cable_id)
        elif action==delete:self.delete_cable(cable_id)


__all__=["PhysicalFibreTopologyDialog"]
