from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QByteArray, Qt
from PySide6.QtGui import (
    QAction,
    QColor,
    QFont,
    QIcon,
    QPainter,
    QPalette,
    QPixmap,
)
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtWidgets import QApplication, QPushButton, QStyle


BOOTSTRAP_BLUE = "#0d6efd"
BOOTSTRAP_INDIGO = "#6610f2"
BOOTSTRAP_GREEN = "#198754"
BOOTSTRAP_CYAN = "#0dcaf0"
BOOTSTRAP_YELLOW = "#ffc107"
BOOTSTRAP_RED = "#dc3545"
INK = "#212529"
MUTED = "#6c757d"
BORDER = "#dee2e6"
SURFACE = "#ffffff"
CANVAS = "#f8f9fa"


_STYLE_ICON_MAP = {
    QStyle.SP_ArrowForward: "arrow-right",
    QStyle.SP_ArrowRight: "arrow-right",
    QStyle.SP_ArrowUp: "arrow-up",
    QStyle.SP_BrowserReload: "arrow-clockwise",
    QStyle.SP_BrowserStop: "eye-slash",
    QStyle.SP_CommandLink: "link-45deg",
    QStyle.SP_ComputerIcon: "pc-display",
    QStyle.SP_DialogApplyButton: "check2",
    QStyle.SP_DialogCancelButton: "x-lg",
    QStyle.SP_DialogOpenButton: "folder2-open",
    QStyle.SP_DialogYesButton: "check-circle",
    QStyle.SP_DirHomeIcon: "building",
    QStyle.SP_DirIcon: "collection",
    QStyle.SP_DirLinkIcon: "diagram-3",
    QStyle.SP_DirOpenIcon: "folder2-open",
    QStyle.SP_DriveHDIcon: "database",
    QStyle.SP_DriveNetIcon: "hdd-network",
    QStyle.SP_FileDialogContentsView: "search",
    QStyle.SP_FileDialogDetailedView: "list-task",
    QStyle.SP_FileDialogListView: "tags",
    QStyle.SP_FileDialogNewFolder: "plus-square",
    QStyle.SP_FileIcon: "file-earmark-plus",
    QStyle.SP_FileLinkIcon: "boxes",
    QStyle.SP_MessageBoxInformation: "info-circle",
    QStyle.SP_MessageBoxQuestion: "question-circle",
    QStyle.SP_MessageBoxWarning: "exclamation-triangle",
    QStyle.SP_TitleBarMaxButton: "arrows-fullscreen",
    QStyle.SP_TrashIcon: "trash3",
}


_ICON_DIR = Path(__file__).resolve().parent / "assets" / "bootstrap-icons"
_ICON_CACHE = {}


def bootstrap_icon(name: str, color: str = INK, size: int = 20) -> QIcon:
    """Load an official Bootstrap Icons SVG asset from the project."""
    icon_name = name if (_ICON_DIR / f"{name}.svg").exists() else "square"
    key = (icon_name, color, int(size))
    if key in _ICON_CACHE:
        return _ICON_CACHE[key]

    svg_path = _ICON_DIR / f"{icon_name}.svg"
    svg = svg_path.read_text(encoding="utf-8")
    svg = svg.replace('width="16"', f'width="{int(size)}"')
    svg = svg.replace('height="16"', f'height="{int(size)}"')
    svg = svg.replace("currentColor", color)

    renderer = QSvgRenderer(QByteArray(svg.encode("utf-8")))
    if not renderer.isValid():
        fallback = QIcon(str(svg_path))
        _ICON_CACHE[key] = fallback
        return fallback

    pixmap = QPixmap(int(size), int(size))
    pixmap.fill(Qt.transparent)
    painter = QPainter(pixmap)
    renderer.render(painter)
    painter.end()

    icon = QIcon(pixmap)
    _ICON_CACHE[key] = icon
    return icon


def bootstrap_icon_for(icon_enum, color: str = INK, size: int = 20) -> QIcon:
    return bootstrap_icon(_STYLE_ICON_MAP.get(icon_enum, "square"), color=color, size=size)


def set_action_icon(action: QAction, name: str, color: str = INK) -> QAction:
    action.setIcon(bootstrap_icon(name, color=color, size=18))
    return action


def style_button(button: QPushButton, role: str = "secondary") -> QPushButton:
    button.setProperty("role", role)
    button.style().unpolish(button)
    button.style().polish(button)
    return button


def apply_bootstrap_theme(app: QApplication) -> None:
    app.setStyle("Fusion")
    font = QFont("Segoe UI", 9)
    app.setFont(font)

    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(CANVAS))
    palette.setColor(QPalette.WindowText, QColor(INK))
    palette.setColor(QPalette.Base, QColor(SURFACE))
    palette.setColor(QPalette.AlternateBase, QColor("#f1f3f5"))
    palette.setColor(QPalette.ToolTipBase, QColor("#111827"))
    palette.setColor(QPalette.ToolTipText, QColor("#ffffff"))
    palette.setColor(QPalette.Text, QColor(INK))
    palette.setColor(QPalette.Button, QColor(SURFACE))
    palette.setColor(QPalette.ButtonText, QColor(INK))
    palette.setColor(QPalette.BrightText, QColor("#ffffff"))
    palette.setColor(QPalette.Highlight, QColor(BOOTSTRAP_BLUE))
    palette.setColor(QPalette.HighlightedText, QColor("#ffffff"))
    app.setPalette(palette)

    app.setStyleSheet(BOOTSTRAP_APP_STYLESHEET)


BOOTSTRAP_APP_STYLESHEET = f"""
QWidget {{
    color: {INK};
    selection-background-color: {BOOTSTRAP_BLUE};
    selection-color: #ffffff;
}}

QMainWindow, QDialog {{
    background: {CANVAS};
}}

QMenuBar {{
    background: {SURFACE};
    border-bottom: 1px solid {BORDER};
    padding: 2px 8px;
}}

QMenuBar::item {{
    padding: 7px 11px;
    border-radius: 6px;
}}

QMenuBar::item:selected {{
    background: #e9f2ff;
    color: #084298;
}}

QMenu {{
    background: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: 8px;
    padding: 6px;
}}

QMenu::item {{
    padding: 7px 24px 7px 24px;
    border-radius: 6px;
}}

QMenu::item:selected {{
    background: {BOOTSTRAP_BLUE};
    color: #ffffff;
}}

QLabel {{
    color: {INK};
}}

QLabel#StatusCaption {{
    color: {MUTED};
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
}}

QLabel#StatusValue {{
    background: {SURFACE};
    border: 1px solid {BORDER};
    border-radius: 6px;
    padding: 5px 8px;
    color: #343a40;
}}

QPushButton, QToolButton {{
    background: {SURFACE};
    color: {INK};
    border: 1px solid #cfd4da;
    border-radius: 6px;
    padding: 5px 9px;
    min-height: 22px;
    font-weight: 600;
}}

QPushButton:hover, QToolButton:hover {{
    background: #f1f5ff;
    border-color: #9ec5fe;
    color: #084298;
}}

QPushButton:pressed, QToolButton:pressed {{
    background: #dbeafe;
}}

QPushButton:checked, QToolButton:checked {{
    background: {BOOTSTRAP_BLUE};
    border-color: {BOOTSTRAP_BLUE};
    color: #ffffff;
}}

QPushButton[role="primary"] {{
    background: {BOOTSTRAP_BLUE};
    border-color: {BOOTSTRAP_BLUE};
    color: #ffffff;
}}

QPushButton[role="success"] {{
    background: {BOOTSTRAP_GREEN};
    border-color: {BOOTSTRAP_GREEN};
    color: #ffffff;
}}

QPushButton[role="danger"] {{
    background: {BOOTSTRAP_RED};
    border-color: {BOOTSTRAP_RED};
    color: #ffffff;
}}

QLineEdit, QTextEdit, QPlainTextEdit, QSpinBox, QDoubleSpinBox, QComboBox {{
    background: {SURFACE};
    border: 1px solid #ced4da;
    border-radius: 6px;
    padding: 5px 8px;
    min-height: 22px;
}}

QLineEdit:focus, QTextEdit:focus, QPlainTextEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {{
    border: 1px solid #86b7fe;
}}

QComboBox::drop-down {{
    width: 24px;
    border: 0;
}}

QTabWidget::pane {{
    border: 1px solid {BORDER};
    background: {SURFACE};
    border-radius: 8px;
}}

QTabBar::tab {{
    background: transparent;
    color: {MUTED};
    border: 0;
    padding: 7px 12px;
    margin-right: 2px;
    font-weight: 600;
}}

QTabBar::tab:hover {{
    color: #084298;
    background: #e9f2ff;
    border-radius: 6px;
}}

QTabBar::tab:selected {{
    color: {BOOTSTRAP_BLUE};
    background: {SURFACE};
    border-bottom: 2px solid {BOOTSTRAP_BLUE};
}}

QTableWidget, QTableView, QListWidget, QTreeWidget {{
    background: {SURFACE};
    alternate-background-color: #f8f9fa;
    border: 1px solid {BORDER};
    border-radius: 8px;
    gridline-color: #e9ecef;
}}

QHeaderView::section {{
    background: #f1f3f5;
    color: #495057;
    border: 0;
    border-bottom: 1px solid {BORDER};
    padding: 8px;
    font-weight: 700;
}}

QListWidget::item, QTreeWidget::item {{
    padding: 6px;
    border-radius: 5px;
}}

QListWidget::item:selected, QTreeWidget::item:selected {{
    background: #dbeafe;
    color: #084298;
}}

QDockWidget {{
    titlebar-close-icon: none;
    titlebar-normal-icon: none;
    border: 1px solid {BORDER};
}}

QDockWidget::title {{
    background: {SURFACE};
    border-bottom: 1px solid {BORDER};
    padding: 8px;
    font-weight: 700;
}}

QScrollBar:vertical, QScrollBar:horizontal {{
    background: transparent;
    border: 0;
    margin: 2px;
}}

QScrollBar::handle:vertical, QScrollBar::handle:horizontal {{
    background: #ced4da;
    border-radius: 5px;
    min-height: 28px;
    min-width: 28px;
}}

QProgressBar {{
    background: #e9ecef;
    border: 0;
    border-radius: 6px;
    text-align: center;
    min-height: 18px;
}}

QProgressBar::chunk {{
    background: {BOOTSTRAP_BLUE};
    border-radius: 6px;
}}
"""


BOOTSTRAP_RIBBON_STYLESHEET = f"""
QTabWidget#AeroRibbon::pane {{
    border: 1px solid {BORDER};
    border-radius: 8px;
    background: {SURFACE};
}}

QTabWidget#AeroRibbon QTabBar::tab {{
    padding: 7px 16px;
    margin-right: 2px;
}}

QScrollArea#RibbonTabScrollArea {{
    background: {SURFACE};
    border: 0;
}}

QScrollArea#RibbonTabScrollArea QWidget {{
    background: transparent;
}}

QFrame#RibbonGroup {{
    background: #fbfcfe;
    border: 1px solid #e3e7ee;
    border-radius: 8px;
    margin: 0;
}}

QFrame#RibbonGroup:hover {{
    border-color: #cfe2ff;
}}

QLabel#RibbonGroupTitle {{
    color: {MUTED};
    font-size: 11px;
    font-weight: 700;
    padding-top: 0;
}}

QTabWidget#AeroRibbon QToolButton {{
    text-align: left;
    padding: 4px 8px;
}}

QTabWidget#AeroRibbon QToolButton:checked {{
    background: #e9f2ff;
    border-color: {BOOTSTRAP_BLUE};
    color: #084298;
}}

QTabWidget#AeroRibbon QToolButton:hover {{
    background: #f1f5ff;
    border-color: #9ec5fe;
    color: #084298;
}}
"""
