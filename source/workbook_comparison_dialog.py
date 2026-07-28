"""Desktop UI for mapping and comparing two Excel workbooks."""

from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from dialog_sizing import fit_dialog_to_screen
from ui_theme import BOOTSTRAP_GREEN, BOOTSTRAP_RED, bootstrap_icon, style_button
from workbook_compare import (
    ASSEMBLY_FIELDS,
    MAIN_FIELDS,
    ComparisonResult,
    RoomMappingGroup,
    RoomRecord,
    SchemaMapping,
    auto_map_rooms,
    compare_workbooks,
    export_comparison,
    extract_rooms,
    suggest_columns,
    suggest_header,
    suggest_schema,
    validate_schema,
)
from xlsx_workbook import WorkbookData, XlsxError, read_xlsx


class _FieldMappingTable(QTableWidget):
    def __init__(self, definitions, parent=None):
        super().__init__(len(definitions), 2, parent)
        self.definitions = tuple(definitions)
        self.setHorizontalHeaderLabels(["Comparison field", "Workbook column"])
        self.verticalHeader().setVisible(False)
        self.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.setSelectionMode(QAbstractItemView.NoSelection)
        for row, definition in enumerate(self.definitions):
            required = " *" if definition.required else ""
            item = QTableWidgetItem(f"{definition.label}{required}")
            item.setFlags(item.flags() & ~Qt.ItemIsEditable)
            self.setItem(row, 0, item)

    def set_headers(self, headers, suggestions=None):
        suggestions = suggestions or {}
        for row, definition in enumerate(self.definitions):
            combo = QComboBox()
            combo.addItem("Not mapped", None)
            for column, header in enumerate(headers):
                label = header or f"Column {column + 1}"
                combo.addItem(f"{column + 1}: {label}", column)
            selected = suggestions.get(definition.key)
            index = combo.findData(selected)
            combo.setCurrentIndex(max(0, index))
            self.setCellWidget(row, 1, combo)
        self.resizeRowsToContents()

    def mapping(self):
        result = {}
        for row, definition in enumerate(self.definitions):
            combo = self.cellWidget(row, 1)
            result[definition.key] = combo.currentData() if combo else None
        return result


class WorkbookSchemaWidget(QWidget):
    def __init__(self, workbook: WorkbookData, parent=None):
        super().__init__(parent)
        self.workbook = workbook
        self.suggested = suggest_schema(workbook)

        layout = QVBoxLayout(self)
        source = QLabel(str(Path(workbook.path)))
        source.setWordWrap(True)
        layout.addWidget(source)

        main_group = QGroupBox("Main asset sheet")
        main_layout = QVBoxLayout(main_group)
        main_form = QFormLayout()
        self.main_sheet = QComboBox()
        self.main_sheet.addItems(workbook.sheet_names)
        self.main_header = QSpinBox()
        self.main_header.setRange(1, 1000)
        main_form.addRow("Worksheet", self.main_sheet)
        main_form.addRow("Header row", self.main_header)
        main_layout.addLayout(main_form)
        self.main_fields = _FieldMappingTable(MAIN_FIELDS)
        main_layout.addWidget(self.main_fields)
        layout.addWidget(main_group)

        assembly_group = QGroupBox("Assembly expansion (optional)")
        assembly_layout = QVBoxLayout(assembly_group)
        assembly_help = QLabel(
            "Choose “No assembly sheet” if assemblies are not available. "
            "Otherwise, parent rows are expanded from the selected item sheet."
        )
        assembly_help.setWordWrap(True)
        assembly_layout.addWidget(assembly_help)
        assembly_form = QFormLayout()
        self.assembly_sheet = QComboBox()
        self.assembly_sheet.addItem("No assembly sheet", None)
        for name in workbook.sheet_names:
            self.assembly_sheet.addItem(name, name)
        self.assembly_header = QSpinBox()
        self.assembly_header.setRange(1, 1000)
        assembly_form.addRow("Worksheet", self.assembly_sheet)
        assembly_form.addRow("Header row", self.assembly_header)
        assembly_layout.addLayout(assembly_form)
        self.assembly_fields = _FieldMappingTable(ASSEMBLY_FIELDS)
        assembly_layout.addWidget(self.assembly_fields)
        layout.addWidget(assembly_group)

        self.main_sheet.currentTextChanged.connect(self._main_sheet_changed)
        self.main_header.valueChanged.connect(self._main_header_changed)
        self.assembly_sheet.currentIndexChanged.connect(
            self._assembly_sheet_changed
        )
        self.assembly_header.valueChanged.connect(self._assembly_header_changed)

        self.main_sheet.setCurrentText(self.suggested.main_sheet)
        self.main_header.setValue(self.suggested.main_header_row + 1)
        assembly_index = self.assembly_sheet.findData(self.suggested.assembly_sheet)
        self.assembly_sheet.setCurrentIndex(max(0, assembly_index))
        self.assembly_header.setValue(self.suggested.assembly_header_row + 1)
        self._refresh_main(self.suggested.main_columns)
        self._refresh_assembly(self.suggested.assembly_columns)

    def _refresh_main(self, suggestions=None):
        sheet = self.workbook.sheet(self.main_sheet.currentText())
        row = self.main_header.value() - 1
        headers = list(sheet.rows[row]) if 0 <= row < len(sheet.rows) else []
        if suggestions is None:
            suggestions = suggest_columns(
                [str(value or "") for value in headers], MAIN_FIELDS
            )
        self.main_fields.set_headers(headers, suggestions)

    def _refresh_assembly(self, suggestions=None):
        enabled = self.assembly_sheet.currentData() is not None
        self.assembly_header.setEnabled(enabled)
        self.assembly_fields.setEnabled(enabled)
        if not enabled:
            self.assembly_fields.set_headers([], {})
            return
        sheet = self.workbook.sheet(self.assembly_sheet.currentData())
        row = self.assembly_header.value() - 1
        headers = list(sheet.rows[row]) if 0 <= row < len(sheet.rows) else []
        if suggestions is None:
            suggestions = suggest_columns(
                [str(value or "") for value in headers], ASSEMBLY_FIELDS
            )
        self.assembly_fields.set_headers(headers, suggestions)

    def _main_sheet_changed(self, name):
        sheet = self.workbook.sheet(name)
        header, columns, _score = suggest_header(sheet, MAIN_FIELDS)
        self.main_header.blockSignals(True)
        self.main_header.setValue(header + 1)
        self.main_header.blockSignals(False)
        self._refresh_main(columns)

    def _main_header_changed(self, _value):
        self._refresh_main()

    def _assembly_sheet_changed(self, _index):
        name = self.assembly_sheet.currentData()
        if name is None:
            self._refresh_assembly({})
            return
        sheet = self.workbook.sheet(name)
        header, columns, _score = suggest_header(sheet, ASSEMBLY_FIELDS)
        self.assembly_header.blockSignals(True)
        self.assembly_header.setValue(header + 1)
        self.assembly_header.blockSignals(False)
        self._refresh_assembly(columns)

    def _assembly_header_changed(self, _value):
        self._refresh_assembly()

    def schema(self):
        assembly_sheet = self.assembly_sheet.currentData()
        return SchemaMapping(
            main_sheet=self.main_sheet.currentText(),
            main_header_row=self.main_header.value() - 1,
            main_columns=self.main_fields.mapping(),
            assembly_sheet=assembly_sheet,
            assembly_header_row=self.assembly_header.value() - 1,
            assembly_columns=(
                self.assembly_fields.mapping() if assembly_sheet is not None else {}
            ),
        )


class SchemaMappingDialog(QDialog):
    def __init__(self, parent, workbook_a, workbook_b):
        super().__init__(parent)
        self.setWindowTitle("Map workbook sheets and columns")
        self.resize(920, 760)
        layout = QVBoxLayout(self)
        intro = QLabel(
            "Map each workbook independently. Fields marked * are required; "
            "at least one of Room code or Room name must also be mapped."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)
        tabs = QTabWidget()
        self.widget_a = WorkbookSchemaWidget(workbook_a)
        self.widget_b = WorkbookSchemaWidget(workbook_b)
        tabs.addTab(self.widget_a, "Workbook A")
        tabs.addTab(self.widget_b, "Workbook B")
        layout.addWidget(tabs)
        buttons = QDialogButtonBox(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok
        )
        buttons.button(QDialogButtonBox.Ok).setText("Continue to room mapping")
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.workbook_a = workbook_a
        self.workbook_b = workbook_b

    def _accept(self):
        errors_a = validate_schema(self.workbook_a, self.widget_a.schema())
        errors_b = validate_schema(self.workbook_b, self.widget_b.schema())
        if errors_a or errors_b:
            lines = []
            if errors_a:
                lines.append("Workbook A:\n• " + "\n• ".join(errors_a))
            if errors_b:
                lines.append("Workbook B:\n• " + "\n• ".join(errors_b))
            QMessageBox.warning(self, "Column mapping incomplete", "\n\n".join(lines))
            return
        self.accept()

    def schemas(self):
        return self.widget_a.schema(), self.widget_b.schema()


class RoomPairDialog(QDialog):
    def __init__(
        self,
        parent,
        available_a: dict[str, RoomRecord],
        available_b: dict[str, RoomRecord],
        selected_a=(),
        selected_b=(),
    ):
        super().__init__(parent)
        self.setWindowTitle("Map room types / room codes")
        self.resize(780, 520)
        layout = QVBoxLayout(self)
        help_label = QLabel(
            "Select one or more rooms on either side. This supports one-to-one, "
            "many-to-one, one-to-many, and many-to-many comparisons."
        )
        help_label.setWordWrap(True)
        layout.addWidget(help_label)
        lists = QHBoxLayout()
        self.list_a = self._room_list(available_a, selected_a)
        self.list_b = self._room_list(available_b, selected_b)
        group_a = QGroupBox("Workbook A rooms")
        group_a_layout = QVBoxLayout(group_a)
        group_a_layout.addWidget(self.list_a)
        group_b = QGroupBox("Workbook B rooms")
        group_b_layout = QVBoxLayout(group_b)
        group_b_layout.addWidget(self.list_b)
        lists.addWidget(group_a)
        lists.addWidget(group_b)
        layout.addLayout(lists)
        self.label_edit = QLineEdit()
        self.label_edit.setPlaceholderText("Optional mapping label")
        layout.addWidget(self.label_edit)
        buttons = QDialogButtonBox(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok
        )
        buttons.accepted.connect(self._accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    @staticmethod
    def _room_list(rooms, selected):
        widget = QListWidget()
        widget.setSelectionMode(QAbstractItemView.MultiSelection)
        selected = set(selected)
        for key, room in rooms.items():
            item = QListWidgetItem(room.label)
            item.setData(Qt.UserRole, key)
            widget.addItem(item)
            if key in selected:
                item.setSelected(True)
        return widget

    def _accept(self):
        if not self.selected_a() and not self.selected_b():
            QMessageBox.warning(
                self, "No rooms selected", "Select at least one room."
            )
            return
        self.accept()

    def selected_a(self):
        return tuple(item.data(Qt.UserRole) for item in self.list_a.selectedItems())

    def selected_b(self):
        return tuple(item.data(Qt.UserRole) for item in self.list_b.selectedItems())


class RoomMappingDialog(QDialog):
    def __init__(self, parent, rooms_a, rooms_b):
        super().__init__(parent)
        self.setWindowTitle("Map room types and room codes")
        self.resize(980, 620)
        self.rooms_a = dict(rooms_a)
        self.rooms_b = dict(rooms_b)
        self.groups = auto_map_rooms(self.rooms_a, self.rooms_b)

        layout = QVBoxLayout(self)
        intro = QLabel(
            "Exact and near-exact room codes/names have been paired automatically. "
            "Add or edit rows to map multiple disparate names or codes together."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Mapping", "Workbook A rooms", "Workbook B rooms"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.table.doubleClicked.connect(self._edit)
        layout.addWidget(self.table)
        actions = QHBoxLayout()
        add_button = QPushButton("Add mapping")
        add_button.setIcon(bootstrap_icon("plus-square"))
        add_button.clicked.connect(self._add)
        edit_button = QPushButton("Edit selected")
        edit_button.setIcon(bootstrap_icon("pencil-square"))
        edit_button.clicked.connect(self._edit)
        remove_button = QPushButton("Remove selected")
        remove_button.setIcon(bootstrap_icon("trash3", BOOTSTRAP_RED))
        remove_button.clicked.connect(self._remove)
        auto_button = QPushButton("Auto-map remaining")
        auto_button.setIcon(bootstrap_icon("arrow-clockwise"))
        auto_button.clicked.connect(self._auto_map_remaining)
        actions.addWidget(add_button)
        actions.addWidget(edit_button)
        actions.addWidget(remove_button)
        actions.addWidget(auto_button)
        actions.addStretch(1)
        layout.addLayout(actions)
        self.include_unmapped = QCheckBox(
            "Include every unmapped room as a one-sided comparison"
        )
        self.include_unmapped.setChecked(True)
        layout.addWidget(self.include_unmapped)
        buttons = QDialogButtonBox(
            QDialogButtonBox.Cancel | QDialogButtonBox.Ok
        )
        buttons.button(QDialogButtonBox.Ok).setText("Use these mappings")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self._refresh()

    def _used(self, skip=None):
        used_a, used_b = set(), set()
        for index, group in enumerate(self.groups):
            if index == skip:
                continue
            used_a.update(group.rooms_a)
            used_b.update(group.rooms_b)
        return used_a, used_b

    def _available(self, skip=None):
        used_a, used_b = self._used(skip)
        return (
            {key: room for key, room in self.rooms_a.items() if key not in used_a},
            {key: room for key, room in self.rooms_b.items() if key not in used_b},
        )

    def _refresh(self):
        self.table.setRowCount(len(self.groups))
        for row, group in enumerate(self.groups):
            label = group.label or f"Mapping {row + 1}"
            values = (
                label,
                "; ".join(self.rooms_a[key].label for key in group.rooms_a),
                "; ".join(self.rooms_b[key].label for key in group.rooms_b),
            )
            for column, value in enumerate(values):
                self.table.setItem(row, column, QTableWidgetItem(value))
        self.table.resizeRowsToContents()

    def _add(self):
        available_a, available_b = self._available()
        dialog = RoomPairDialog(self, available_a, available_b)
        if dialog.exec() == QDialog.Accepted:
            self.groups.append(
                RoomMappingGroup(
                    dialog.selected_a(),
                    dialog.selected_b(),
                    dialog.label_edit.text().strip(),
                )
            )
            self._refresh()

    def _selected_row(self):
        rows = self.table.selectionModel().selectedRows()
        return rows[0].row() if rows else -1

    def _edit(self, *_args):
        row = self._selected_row()
        if row < 0:
            return
        group = self.groups[row]
        available_a, available_b = self._available(row)
        dialog = RoomPairDialog(
            self, available_a, available_b, group.rooms_a, group.rooms_b
        )
        dialog.label_edit.setText(group.label)
        if dialog.exec() == QDialog.Accepted:
            self.groups[row] = RoomMappingGroup(
                dialog.selected_a(),
                dialog.selected_b(),
                dialog.label_edit.text().strip(),
            )
            self._refresh()
            self.table.selectRow(row)

    def _remove(self):
        row = self._selected_row()
        if row >= 0:
            self.groups.pop(row)
            self._refresh()

    def _auto_map_remaining(self):
        available_a, available_b = self._available()
        self.groups.extend(auto_map_rooms(available_a, available_b))
        self._refresh()


class WorkbookComparisonDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Compare Excel room asset schedules")
        self.resize(1180, 780)
        self.workbook_a: WorkbookData | None = None
        self.workbook_b: WorkbookData | None = None
        self.schema_a: SchemaMapping | None = None
        self.schema_b: SchemaMapping | None = None
        self.room_groups: list[RoomMappingGroup] = []
        self.room_mapping_configured = False
        self.include_unmapped = True
        self.result: ComparisonResult | None = None

        layout = QVBoxLayout(self)
        intro = QLabel(
            "Import two Excel workbooks, map their sheets and columns, pair room "
            "types/codes, then compare a fully expanded asset list."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        sources = QGroupBox("Source workbooks")
        source_layout = QFormLayout(sources)
        self.path_a = QLineEdit()
        self.path_a.setReadOnly(True)
        self.path_b = QLineEdit()
        self.path_b.setReadOnly(True)
        source_layout.addRow("Workbook A", self._path_row(self.path_a, self._browse_a))
        source_layout.addRow("Workbook B", self._path_row(self.path_b, self._browse_b))
        layout.addWidget(sources)

        steps = QHBoxLayout()
        self.schema_button = QPushButton("1. Map sheets and columns")
        self.schema_button.setIcon(bootstrap_icon("list-task"))
        self.schema_button.clicked.connect(self._configure_schema)
        self.rooms_button = QPushButton("2. Map room types / codes")
        self.rooms_button.setIcon(bootstrap_icon("diagram-3"))
        self.rooms_button.clicked.connect(self._configure_rooms)
        self.compare_button = QPushButton("3. Compare assets")
        self.compare_button.setIcon(bootstrap_icon("check-circle", BOOTSTRAP_GREEN))
        style_button(self.compare_button, "success")
        self.compare_button.clicked.connect(self._compare)
        steps.addWidget(self.schema_button)
        steps.addWidget(self.rooms_button)
        steps.addWidget(self.compare_button)
        steps.addStretch(1)
        layout.addLayout(steps)

        result_bar = QHBoxLayout()
        self.summary = QLabel("Import both workbooks to begin.")
        self.show_unchanged = QCheckBox("Show unchanged assets")
        self.show_unchanged.stateChanged.connect(self._populate_results)
        result_bar.addWidget(self.summary)
        result_bar.addStretch(1)
        result_bar.addWidget(self.show_unchanged)
        layout.addLayout(result_bar)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(
            [
                "Asset / group",
                "Description A",
                "Description B",
                "Qty A",
                "Qty B",
                "Difference",
                "Status",
            ]
        )
        self.tree.setAlternatingRowColors(True)
        self.tree.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tree.header().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.tree.header().setSectionResizeMode(1, QHeaderView.Stretch)
        self.tree.header().setSectionResizeMode(2, QHeaderView.Stretch)
        for column in range(3, 7):
            self.tree.header().setSectionResizeMode(column, QHeaderView.ResizeToContents)
        layout.addWidget(self.tree)

        bottom = QHBoxLayout()
        self.export_button = QPushButton("Export comparison to Excel")
        self.export_button.setIcon(bootstrap_icon("database"))
        self.export_button.clicked.connect(self._export)
        bottom.addWidget(self.export_button)
        bottom.addStretch(1)
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        bottom.addWidget(close_button)
        layout.addLayout(bottom)
        self._update_buttons()
        fit_dialog_to_screen(self)

    def _path_row(self, line_edit, callback):
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(line_edit)
        button = QPushButton("Browse…")
        button.setIcon(bootstrap_icon("folder2-open"))
        button.clicked.connect(callback)
        layout.addWidget(button)
        return widget

    def _browse_a(self):
        self._browse("a")

    def _browse_b(self):
        self._browse("b")

    def _browse(self, side):
        path, _ = QFileDialog.getOpenFileName(
            self,
            f"Select workbook {side.upper()}",
            "",
            "Excel workbooks (*.xlsx *.xlsm)",
        )
        if not path:
            return
        try:
            workbook = read_xlsx(path)
        except XlsxError as exc:
            QMessageBox.critical(self, "Workbook import failed", str(exc))
            return
        if side == "a":
            self.workbook_a = workbook
            self.path_a.setText(path)
        else:
            self.workbook_b = workbook
            self.path_b.setText(path)
        self.schema_a = self.schema_b = None
        self.room_groups = []
        self.room_mapping_configured = False
        self.result = None
        self.tree.clear()
        self.summary.setText(
            f"Loaded {Path(path).name} ({len(workbook.sheets)} worksheets)."
        )
        self._update_buttons()
        if self.workbook_a is not None and self.workbook_b is not None:
            self._configure_schema()

    def _update_buttons(self):
        both = self.workbook_a is not None and self.workbook_b is not None
        self.schema_button.setEnabled(both)
        schemas = self.schema_a is not None and self.schema_b is not None
        self.rooms_button.setEnabled(both and schemas)
        self.compare_button.setEnabled(
            both and schemas and self.room_mapping_configured
        )
        self.export_button.setEnabled(self.result is not None)

    def _configure_schema(self):
        if self.workbook_a is None or self.workbook_b is None:
            return False
        dialog = SchemaMappingDialog(self, self.workbook_a, self.workbook_b)
        if dialog.exec() != QDialog.Accepted:
            return False
        self.schema_a, self.schema_b = dialog.schemas()
        self.room_groups = []
        self.room_mapping_configured = False
        self.result = None
        self.tree.clear()
        self.summary.setText("Column mappings saved. Map rooms before comparing.")
        self._update_buttons()
        self._configure_rooms()
        return True

    def _configure_rooms(self):
        if (
            self.workbook_a is None
            or self.workbook_b is None
            or self.schema_a is None
            or self.schema_b is None
        ):
            return False
        rooms_a = extract_rooms(self.workbook_a, self.schema_a)
        rooms_b = extract_rooms(self.workbook_b, self.schema_b)
        dialog = RoomMappingDialog(self, rooms_a, rooms_b)
        if self.room_mapping_configured:
            dialog.groups = list(self.room_groups)
            dialog.include_unmapped.setChecked(self.include_unmapped)
            dialog._refresh()
        if dialog.exec() != QDialog.Accepted:
            return False
        self.room_groups = list(dialog.groups)
        self.room_mapping_configured = True
        self.include_unmapped = dialog.include_unmapped.isChecked()
        self.result = None
        self.tree.clear()
        self.summary.setText(
            f"Saved {len(self.room_groups)} explicit room mapping(s)."
        )
        self._update_buttons()
        return True

    def _compare(self):
        if (
            self.workbook_a is None
            or self.workbook_b is None
            or self.schema_a is None
            or self.schema_b is None
        ):
            return
        self.result = compare_workbooks(
            self.workbook_a,
            self.schema_a,
            self.workbook_b,
            self.schema_b,
            self.room_groups,
            include_unmapped=self.include_unmapped,
        )
        self._populate_results()
        warnings = sum(len(mapping.warnings) for mapping in self.result.mappings)
        warning_text = f" {warnings} assembly warning(s)." if warnings else ""
        self.summary.setText(
            f"{len(self.result.mappings)} room comparison(s); "
            f"{self.result.difference_count} total-asset difference(s)."
            f"{warning_text}"
        )
        self._update_buttons()

    @staticmethod
    def _quantity(value):
        return f"{value:f}".rstrip("0").rstrip(".") if value else "0"

    def _add_rows(self, parent, rows):
        visible = 0
        assembly_nodes = {}
        for row in rows:
            if not self.show_unchanged.isChecked() and row.status == "Same":
                continue
            group_label = row.assembly_group
            if row.level != "Total asset list":
                ids = []
                if row.assembly_ids_a:
                    ids.append(f"A: {row.assembly_ids_a}")
                if row.assembly_ids_b:
                    ids.append(f"B: {row.assembly_ids_b}")
                if ids:
                    group_label = f"{row.assembly_group} — {' | '.join(ids)}"
            group = assembly_nodes.get(group_label)
            if group is None:
                group = QTreeWidgetItem(parent, [group_label])
                assembly_nodes[group_label] = group
            item = QTreeWidgetItem(
                group,
                [
                    row.asset_code or "(no asset code)",
                    row.description_a,
                    row.description_b,
                    self._quantity(row.quantity_a),
                    self._quantity(row.quantity_b),
                    self._quantity(row.difference),
                    row.status,
                ],
            )
            colour = {
                "Added": QColor("#d1e7dd"),
                "Removed": QColor("#f8d7da"),
                "Quantity changed": QColor("#fff3cd"),
                "Changed": QColor("#fff3cd"),
            }.get(row.status)
            if colour:
                for column in range(self.tree.columnCount()):
                    item.setBackground(column, colour)
            visible += 1
        if visible == 0:
            QTreeWidgetItem(parent, ["No visible differences"])

    def _populate_results(self, *_args):
        self.tree.clear()
        if self.result is None:
            return
        for mapping in self.result.mappings:
            rooms_a = "; ".join(room.label for room in mapping.rooms_a) or "—"
            rooms_b = "; ".join(room.label for room in mapping.rooms_b) or "—"
            root = QTreeWidgetItem(
                self.tree, [f"{mapping.label}: {rooms_a}  ↔  {rooms_b}"]
            )
            totals = QTreeWidgetItem(root, ["Total expanded asset list"])
            self._add_rows(totals, mapping.total_rows)
            breakdown = QTreeWidgetItem(root, ["Assembly breakdown (grouped by Assembly ID)"])
            self._add_rows(breakdown, mapping.assembly_rows)
            if mapping.warnings:
                warning = QTreeWidgetItem(
                    root, [f"Warnings ({len(mapping.warnings)})"]
                )
                for text in mapping.warnings:
                    QTreeWidgetItem(warning, [text])
        self.tree.expandToDepth(1)

    def _export(self):
        if (
            self.result is None
            or self.workbook_a is None
            or self.workbook_b is None
            or self.schema_a is None
            or self.schema_b is None
        ):
            return
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export workbook comparison",
            "workbook_asset_comparison.xlsx",
            "Excel workbook (*.xlsx)",
        )
        if not path:
            return
        try:
            destination = export_comparison(
                path,
                self.result,
                self.workbook_a,
                self.schema_a,
                self.workbook_b,
                self.schema_b,
            )
        except XlsxError as exc:
            QMessageBox.critical(self, "Export failed", str(exc))
            return
        QMessageBox.information(
            self,
            "Export complete",
            "The workbook contains Original A, Original B, and Differences tabs.\n\n"
            f"{destination}",
        )
