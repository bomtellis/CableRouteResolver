from __future__ import annotations

from PySide6.QtCore import QObject, QEvent, QPoint, QSize, Qt
from PySide6.QtGui import QColor, QIcon, QPainter, QPixmap, QPolygon
from PySide6.QtWidgets import (
    QAbstractButton,
    QApplication,
    QDialog,
    QDialogButtonBox,
    QScrollArea,
    QTabBar,
    QToolButton,
    QSizePolicy,
    QWidget,
)


def _arrow_icon(arrow_type: Qt.ArrowType) -> QIcon:
    pixmap = QPixmap(16, 16)
    pixmap.fill(Qt.transparent)
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing)
    painter.setBrush(QColor("#495057"))
    painter.setPen(Qt.NoPen)
    if arrow_type == Qt.LeftArrow:
        points = [QPoint(5, 8), QPoint(11, 3), QPoint(11, 13)]
    elif arrow_type == Qt.RightArrow:
        points = [QPoint(11, 8), QPoint(5, 3), QPoint(5, 13)]
    elif arrow_type == Qt.UpArrow:
        points = [QPoint(8, 5), QPoint(3, 11), QPoint(13, 11)]
    else:
        points = [QPoint(8, 11), QPoint(3, 5), QPoint(13, 5)]
    painter.drawPolygon(QPolygon(points))
    painter.end()
    return QIcon(pixmap)


def available_dialog_size(dialog: QWidget, margin_x: int = 80, margin_y: int = 100) -> QSize:
    screen = dialog.screen() or QApplication.primaryScreen()
    if screen is None:
        return QSize(1840, 980)
    available = screen.availableGeometry()
    return QSize(
        max(360, available.width() - margin_x),
        max(320, available.height() - margin_y),
    )


def fit_dialog_to_screen(dialog: QDialog, margin_x: int = 80, margin_y: int = 100) -> None:
    maximum = available_dialog_size(dialog, margin_x, margin_y)
    minimum = dialog.minimumSize()
    dialog.setMinimumSize(min(minimum.width(), maximum.width()), min(minimum.height(), maximum.height()))
    dialog.setMaximumSize(maximum)

    width = min(max(dialog.width(), dialog.sizeHint().width()), maximum.width())
    height = min(max(dialog.height(), dialog.sizeHint().height()), maximum.height())
    dialog.resize(width, height)

    screen = dialog.screen() or QApplication.primaryScreen()
    if screen is not None:
        frame = dialog.frameGeometry()
        frame.moveCenter(screen.availableGeometry().center())
        dialog.move(frame.topLeft())


def prepare_dialog_buttons(dialog: QDialog) -> None:
    for button in dialog.findChildren(QAbstractButton):
        if isinstance(button, QToolButton) and not button.text().strip() and button.icon().isNull():
            arrow_type = button.arrowType()
            if arrow_type != Qt.NoArrow:
                button.setArrowType(Qt.NoArrow)
                button.setIcon(_arrow_icon(arrow_type))

        icon = button.icon()
        if icon.isNull():
            continue
        current = button.iconSize()
        if current.width() < 18 or current.height() < 18:
            button.setIconSize(QSize(18, 18))
        if not button.text().strip():
            button.setMinimumSize(max(button.minimumWidth(), 34), max(button.minimumHeight(), 32))
            if isinstance(button, QToolButton):
                button.setToolButtonStyle(Qt.ToolButtonIconOnly)

    for button_box in dialog.findChildren(QDialogButtonBox):
        for button in button_box.buttons():
            if not button.icon().isNull():
                button.setIconSize(QSize(18, 18))


def prepare_tab_scroll_buttons(root: QWidget) -> None:
    tab_bars = [root] if isinstance(root, QTabBar) else root.findChildren(QTabBar)
    for tab_bar in tab_bars:
        for button in tab_bar.findChildren(QToolButton):
            arrow_type = button.arrowType()
            if arrow_type == Qt.NoArrow and button.icon().isNull():
                continue
            if not button.text().strip() and button.icon().isNull():
                button.setIcon(_arrow_icon(arrow_type))
            if arrow_type != Qt.NoArrow:
                button.setArrowType(Qt.NoArrow)
            button.setIconSize(QSize(14, 14))
            button.setFixedSize(30, 30)
            button.setMinimumSize(30, 30)
            button.setMaximumSize(30, 30)
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
            button.setToolButtonStyle(Qt.ToolButtonIconOnly)


class _DialogScreenGuard(QObject):
    def eventFilter(self, obj, event):  # noqa: ANN001
        if isinstance(obj, QDialog) and event.type() == QEvent.Show:
            prepare_dialog_buttons(obj)
            prepare_tab_scroll_buttons(obj)
            fit_dialog_to_screen(obj)
            obj.setSizeGripEnabled(True)
        elif isinstance(obj, QTabBar) and event.type() in {QEvent.Show, QEvent.LayoutRequest, QEvent.ChildAdded}:
            prepare_tab_scroll_buttons(obj)
        return super().eventFilter(obj, event)


def install_dialog_screen_guard(app: QApplication) -> None:
    guard = getattr(app, "_dialog_screen_guard", None)
    if guard is None:
        guard = _DialogScreenGuard(app)
        app._dialog_screen_guard = guard
        app.installEventFilter(guard)


def configure_scroll_area(scroll: QScrollArea, max_height: int | None = None) -> QScrollArea:
    scroll.setWidgetResizable(True)
    scroll.setFrameShape(QScrollArea.NoFrame)
    scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
    scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
    if max_height is not None:
        scroll.setMaximumHeight(max_height)
    return scroll
