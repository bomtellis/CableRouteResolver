"""Multi-step room-type to asset-bundle assignment workflow."""

from __future__ import annotations

from copy import deepcopy
import re

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWizard,
    QWizardPage,
)

from asset_bundles import (
    clean_bundle_assignments,
    normalise_asset_bundles,
    room_bundle_asset_overlaps,
)
from room_bundle_matrix import (
    export_room_bundle_matrix,
    import_room_bundle_matrix,
)
from xlsx_workbook import XlsxError


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _room_id(room_type, index=0) -> str:
    return _text(room_type.get("id")) or f"ROOM-{index + 1}"


def _room_name(room_type) -> str:
    return _text(room_type.get("name")) or _text(room_type.get("id"))


def _bundle_label(bundle) -> str:
    bundle_id = _text(bundle.get("id"))
    name = _text(bundle.get("name")) or bundle_id
    return f"{name} [{bundle_id}]"


class _SelectionPage(QWizardPage):
    def __init__(self, room_types, bundles):
        super().__init__()
        self.setTitle("Select room types and bundles")
        self.setSubTitle(
            "Choose the rows and columns to include in the assignment matrix."
        )
        self.room_types = room_types
        self.bundles = bundles
        self._applying_check_state = False
        layout = QHBoxLayout(self)
        room_column = QVBoxLayout()
        room_column.addWidget(QLabel("Room types"))
        room_filter = QLineEdit()
        room_filter.setPlaceholderText("Filter room types...")
        room_filter.setClearButtonEnabled(True)
        room_column.addWidget(room_filter)
        room_column.addWidget(
            QLabel(
                "Paste room type IDs or exact names from Excel "
                "(one row or cell per room type)"
            )
        )
        self.room_list_edit = QPlainTextEdit()
        self.room_list_edit.setPlaceholderText(
            "Paste an Excel column here, for example:\n"
            "OFFICE\nMEETING\nGeneral Store"
        )
        self.room_list_edit.setMaximumHeight(90)
        room_column.addWidget(self.room_list_edit)
        self.list_match_status_label = QLabel()
        self.list_match_status_label.setWordWrap(True)
        room_column.addWidget(self.list_match_status_label)
        room_list_buttons = QHBoxLayout()
        self.add_list_button = QPushButton("Add Listed Room Types")
        self.clear_list_button = QPushButton("Clear List")
        self.add_list_button.setEnabled(False)
        self.add_list_button.clicked.connect(self._add_listed_room_types)
        self.clear_list_button.clicked.connect(self._clear_room_list)
        self.room_list_edit.textChanged.connect(self._refresh_list_button)
        room_list_buttons.addWidget(self.add_list_button)
        room_list_buttons.addWidget(self.clear_list_button)
        room_list_buttons.addStretch(1)
        room_column.addLayout(room_list_buttons)
        self.room_table = self._selection_table(
            [
                (
                    index,
                    _room_id(room, index),
                    _room_name(room),
                    "",
                )
                for index, room in enumerate(room_types)
            ],
            ["Use", "Room Type ID", "Room Type Name"],
        )
        room_column.addWidget(self.room_table, 1)
        room_buttons = QHBoxLayout()
        room_all = QPushButton("Select all")
        room_none = QPushButton("Clear")
        room_all.clicked.connect(lambda: self._set_all(self.room_table, True))
        room_none.clicked.connect(lambda: self._set_all(self.room_table, False))
        room_buttons.addWidget(room_all)
        room_buttons.addWidget(room_none)
        room_buttons.addStretch(1)
        room_column.addLayout(room_buttons)
        room_filter.textChanged.connect(
            lambda value: self._filter(self.room_table, value)
        )
        layout.addLayout(room_column, 1)

        bundle_column = QVBoxLayout()
        bundle_column.addWidget(QLabel("Asset bundles"))
        bundle_filter = QLineEdit()
        bundle_filter.setPlaceholderText("Filter bundles...")
        bundle_filter.setClearButtonEnabled(True)
        bundle_column.addWidget(bundle_filter)
        self.bundle_table = self._selection_table(
            [
                (
                    index,
                    _text(bundle.get("id")),
                    _text(bundle.get("name")),
                    _text(bundle.get("description")),
                )
                for index, bundle in enumerate(bundles)
            ],
            ["Use", "Bundle ID", "Bundle Name", "Description"],
        )
        bundle_column.addWidget(self.bundle_table, 1)
        bundle_buttons = QHBoxLayout()
        bundle_all = QPushButton("Select all")
        bundle_none = QPushButton("Clear")
        bundle_all.clicked.connect(
            lambda: self._set_all(self.bundle_table, True)
        )
        bundle_none.clicked.connect(
            lambda: self._set_all(self.bundle_table, False)
        )
        bundle_buttons.addWidget(bundle_all)
        bundle_buttons.addWidget(bundle_none)
        bundle_buttons.addStretch(1)
        bundle_column.addLayout(bundle_buttons)
        bundle_filter.textChanged.connect(
            lambda value: self._filter(self.bundle_table, value)
        )
        layout.addLayout(bundle_column, 1)

    def _selection_table(self, rows, headers):
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setSelectionBehavior(QAbstractItemView.SelectRows)
        table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setSectionResizeMode(
            len(headers) - 1, QHeaderView.Stretch
        )
        table.setColumnWidth(0, 48)
        for source_index, identity, name, description in rows:
            row = table.rowCount()
            table.insertRow(row)
            check = QTableWidgetItem()
            check.setData(Qt.UserRole, source_index)
            check.setFlags(
                Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsUserCheckable
            )
            check.setCheckState(Qt.Checked)
            table.setItem(row, 0, check)
            values = [identity, name]
            if len(headers) > 3:
                values.append(description)
            for column, value in enumerate(values, start=1):
                table.setItem(row, column, QTableWidgetItem(value))
        table.itemChanged.connect(
            lambda item, current_table=table: self._apply_check_to_selected_rows(
                current_table, item
            )
        )
        return table

    def _apply_check_to_selected_rows(self, table, changed_item):
        if self._applying_check_state or changed_item.column() != 0:
            return

        selected_rows = {
            index.row() for index in table.selectionModel().selectedRows()
        }
        if changed_item.row() not in selected_rows or len(selected_rows) < 2:
            return

        self._applying_check_state = True
        try:
            state = changed_item.checkState()
            for row in selected_rows:
                check_item = table.item(row, 0)
                if check_item is not None and check_item.checkState() != state:
                    check_item.setCheckState(state)
        finally:
            self._applying_check_state = False

    def _set_all(self, table, checked):
        state = Qt.Checked if checked else Qt.Unchecked
        for row in range(table.rowCount()):
            if not table.isRowHidden(row):
                table.item(row, 0).setCheckState(state)

    def _room_label(self, index):
        room_type = self.room_types[index]
        room_type_id = _text(room_type.get("id"))
        name = _text(room_type.get("name")) or "Unnamed room type"
        return f"{name} [{room_type_id}]" if room_type_id else name

    @staticmethod
    def _pasted_room_tokens(value):
        text = _text(value)
        if not text:
            return []
        tokens = re.split(r"[\t\r\n]+", text)
        if len(tokens) == 1:
            tokens = re.split(r"[;,]+", text)
        result = []
        for token in tokens:
            cleaned = _text(token).strip("\"'")
            if cleaned and cleaned not in result:
                result.append(cleaned)
        return result

    def _match_pasted_room_types(self, value):
        id_matches = {}
        name_matches = {}
        label_matches = {}
        for index, room_type in enumerate(self.room_types):
            room_type_id = _text(room_type.get("id"))
            name = _text(room_type.get("name"))
            if room_type_id:
                id_matches.setdefault(room_type_id.casefold(), []).append(index)
            if name:
                name_matches.setdefault(name.casefold(), []).append(index)
            label_matches.setdefault(
                self._room_label(index).casefold(), []
            ).append(index)

        matched = []
        unmatched = []
        ambiguous = {}
        for token in self._pasted_room_tokens(value):
            key = token.casefold()
            candidates = id_matches.get(key) or label_matches.get(key)
            if not candidates:
                bracket_match = re.search(r"\[([^\]]+)\]\s*$", token)
                if bracket_match:
                    candidates = id_matches.get(
                        bracket_match.group(1).strip().casefold()
                    )
            if not candidates:
                candidates = name_matches.get(key)
            candidates = list(candidates or [])
            if len(candidates) == 1:
                if candidates[0] not in matched:
                    matched.append(candidates[0])
            elif len(candidates) > 1:
                ambiguous[token] = [
                    self._room_label(index) for index in candidates
                ]
            else:
                unmatched.append(token)
        return matched, unmatched, ambiguous

    def _refresh_list_button(self):
        self.add_list_button.setEnabled(
            bool(self.room_list_edit.toPlainText().strip())
        )

    def _clear_room_list(self):
        self.room_list_edit.clear()
        self.list_match_status_label.clear()
        self.list_match_status_label.setStyleSheet("")

    def _add_listed_room_types(self):
        matched, unmatched, ambiguous = self._match_pasted_room_types(
            self.room_list_edit.toPlainText()
        )
        matched_indexes = set(matched)
        self._applying_check_state = True
        signals_were_blocked = self.room_table.blockSignals(True)
        try:
            for row in range(self.room_table.rowCount()):
                check_item = self.room_table.item(row, 0)
                if check_item is None:
                    continue
                source_index = int(check_item.data(Qt.UserRole))
                if source_index in matched_indexes:
                    check_item.setCheckState(Qt.Checked)
        finally:
            self.room_table.blockSignals(signals_were_blocked)
            self._applying_check_state = False

        issue_count = len(unmatched) + len(ambiguous)
        matched_label = (
            f"Matched {len(matched)} room type(s)"
            if matched
            else "No room types matched"
        )
        if not issue_count:
            self.list_match_status_label.setText(
                f"{matched_label}. All pasted entries were recognised."
            )
            self.list_match_status_label.setStyleSheet("color: #287233;")
            return

        issue_parts = []
        if unmatched:
            issue_parts.append(f"{len(unmatched)} unmatched")
        if ambiguous:
            issue_parts.append(f"{len(ambiguous)} ambiguous")
        self.list_match_status_label.setText(
            f"{matched_label}; {', '.join(issue_parts)}. "
            "Review the warning and correct the pasted list."
        )
        self.list_match_status_label.setStyleSheet("color: #9a6700;")

        details = []
        if unmatched:
            details.append("Unmatched entries:\n- " + "\n- ".join(unmatched))
        if ambiguous:
            ambiguous_lines = [
                f"- {token}: {', '.join(labels)}"
                for token, labels in ambiguous.items()
            ]
            details.append(
                "Ambiguous entries (use the room type ID instead):\n"
                + "\n".join(ambiguous_lines)
            )
        QMessageBox.warning(
            self,
            "Some Room Types Were Not Added",
            "\n\n".join(details),
        )

    def _filter(self, table, value):
        terms = [term for term in _text(value).casefold().split() if term]
        for row in range(table.rowCount()):
            text = " ".join(
                _text(table.item(row, column).text())
                for column in range(1, table.columnCount())
                if table.item(row, column) is not None
            ).casefold()
            table.setRowHidden(row, not all(term in text for term in terms))

    def _selected_indexes(self, table):
        return [
            int(table.item(row, 0).data(Qt.UserRole))
            for row in range(table.rowCount())
            if table.item(row, 0).checkState() == Qt.Checked
        ]

    def selected_room_indexes(self):
        return self._selected_indexes(self.room_table)

    def selected_bundle_indexes(self):
        return self._selected_indexes(self.bundle_table)

    def validatePage(self):
        if not self.selected_room_indexes():
            QMessageBox.information(
                self, "Room Bundle Assignment", "Select at least one room type."
            )
            return False
        if not self.selected_bundle_indexes():
            QMessageBox.information(
                self, "Room Bundle Assignment", "Select at least one asset bundle."
            )
            return False
        return True


class _MatrixPage(QWizardPage):
    def __init__(self):
        super().__init__()
        self.setTitle("Assign bundles to room types")
        self.setSubTitle(
            "Enter bundle quantities directly, or import/export an Excel matrix."
        )
        self._signature = None
        self.rooms = []
        self.bundles = []
        layout = QVBoxLayout(self)
        toolbar = QHBoxLayout()
        self.import_button = QPushButton("Import Excel...")
        self.export_button = QPushButton("Export Excel...")
        self.clear_button = QPushButton("Clear matrix")
        self.import_button.clicked.connect(self._import_excel)
        self.export_button.clicked.connect(self._export_excel)
        self.clear_button.clicked.connect(self._clear_matrix)
        toolbar.addWidget(self.import_button)
        toolbar.addWidget(self.export_button)
        toolbar.addWidget(self.clear_button)
        toolbar.addStretch(1)
        layout.addLayout(toolbar)
        note = QLabel(
            "A blank or 0 cell means no assignment. Positive integers are bundle "
            "instance quantities. Unselected bundles remain unchanged."
        )
        note.setWordWrap(True)
        layout.addWidget(note)
        self.table = QTableWidget()
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table, 1)

    def initializePage(self):
        wizard = self.wizard()
        room_indexes = wizard.selection_page.selected_room_indexes()
        bundle_indexes = wizard.selection_page.selected_bundle_indexes()
        signature = (tuple(room_indexes), tuple(bundle_indexes))
        if signature == self._signature:
            return
        self._signature = signature
        self.rooms = [
            (index, wizard.room_types[index]) for index in room_indexes
        ]
        self.bundles = [
            wizard.bundles[index] for index in bundle_indexes
        ]
        self._build_table()

    def _build_table(self):
        self.table.clear()
        self.table.setRowCount(len(self.rooms))
        self.table.setColumnCount(1 + len(self.bundles))
        self.table.setHorizontalHeaderLabels(
            ["Room Type"]
            + [
                f"{_text(bundle.get('id'))}\n{_text(bundle.get('name'))}"
                for bundle in self.bundles
            ]
        )
        self.table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents
        )
        for column in range(1, self.table.columnCount()):
            self.table.horizontalHeader().setSectionResizeMode(
                column, QHeaderView.Interactive
            )
            self.table.setColumnWidth(column, 120)
            item = self.table.horizontalHeaderItem(column)
            bundle = self.bundles[column - 1]
            item.setToolTip(
                f"{_bundle_label(bundle)}\n"
                f"{_text(bundle.get('description'))}"
            )
        for row, (source_index, room) in enumerate(self.rooms):
            room_id = _room_id(room, source_index)
            room_item = QTableWidgetItem(
                f"{room_id} - {_room_name(room)}"
            )
            room_item.setData(Qt.UserRole, room_id)
            room_item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            self.table.setItem(row, 0, room_item)
            existing = {
                assignment["bundle_id"]: int(assignment["qty"])
                for assignment in clean_bundle_assignments(
                    room.get("asset_bundle_assignments", [])
                )
            }
            for column, bundle in enumerate(self.bundles, start=1):
                value = existing.get(_text(bundle.get("id")), 0)
                item = QTableWidgetItem(str(value) if value > 0 else "")
                item.setTextAlignment(Qt.AlignCenter)
                item.setToolTip(
                    "Blank/0 = not applied; positive integer = bundle quantity"
                )
                self.table.setItem(row, column, item)

    def _cell_quantity(self, row, column):
        value = _text(self.table.item(row, column).text())
        if not value:
            return 0
        try:
            quantity = int(value)
        except ValueError:
            return None
        return quantity if 0 <= quantity <= 100000 else None

    def quantities(self, *, show_errors=True):
        result = {}
        errors = []
        for row, (source_index, room) in enumerate(self.rooms):
            room_id = _room_id(room, source_index)
            result[room_id] = {}
            for column, bundle in enumerate(self.bundles, start=1):
                quantity = self._cell_quantity(row, column)
                if quantity is None:
                    errors.append(
                        f"{room_id} / {_text(bundle.get('id'))}"
                    )
                    continue
                result[room_id][_text(bundle.get("id"))] = quantity
        if errors and show_errors:
            QMessageBox.warning(
                self,
                "Room Bundle Assignment",
                "Enter whole-number quantities from 0 to 100000 for:\n\n"
                + "\n".join(errors[:20]),
            )
        return None if errors else result

    def validatePage(self):
        return self.quantities() is not None

    def _clear_matrix(self):
        for row in range(self.table.rowCount()):
            for column in range(1, self.table.columnCount()):
                self.table.item(row, column).setText("")

    def _export_excel(self):
        quantities = self.quantities()
        if quantities is None:
            return
        path, _selected_filter = QFileDialog.getSaveFileName(
            self,
            "Export Room Type Bundle Matrix",
            "room-type-bundle-matrix.xlsx",
            "Excel workbooks (*.xlsx)",
        )
        if not path:
            return
        try:
            output = export_room_bundle_matrix(
                path,
                [room for _index, room in self.rooms],
                self.bundles,
                quantities,
            )
        except XlsxError as exc:
            QMessageBox.critical(self, "Export failed", str(exc))
            return
        QMessageBox.information(
            self,
            "Export complete",
            f"Room type bundle matrix written to:\n{output}",
        )

    def _import_excel(self):
        path, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Import Room Type Bundle Matrix",
            "",
            "Excel workbooks (*.xlsx *.xlsm)",
        )
        if not path:
            return
        try:
            imported = import_room_bundle_matrix(
                path,
                [room for _index, room in self.rooms],
                self.bundles,
            )
        except XlsxError as exc:
            QMessageBox.critical(self, "Import failed", str(exc))
            return
        row_by_id = {
            _room_id(room, source_index): row
            for row, (source_index, room) in enumerate(self.rooms)
        }
        column_by_id = {
            _text(bundle.get("id")): column
            for column, bundle in enumerate(self.bundles, start=1)
        }
        for room_id in imported["matched_room_ids"]:
            row = row_by_id.get(room_id)
            if row is None:
                continue
            for bundle_id in imported["matched_bundle_ids"]:
                column = column_by_id.get(bundle_id)
                if column is not None:
                    self.table.item(row, column).setText("")
        for room_id, assignments in imported["assignments"].items():
            row = row_by_id.get(room_id)
            if row is None:
                continue
            for bundle_id, quantity in assignments.items():
                column = column_by_id.get(bundle_id)
                if column is None:
                    continue
                self.table.item(row, column).setText(
                    str(quantity) if quantity > 0 else ""
                )
        message = (
            f"Imported {len(imported['matched_room_ids'])} room type(s) and "
            f"{len(imported['matched_bundle_ids'])} bundle column(s) from "
            f"{imported['sheet']} ({imported['layout']} layout)."
        )
        if imported["warnings"]:
            message += "\n\nWarnings:\n" + "\n".join(imported["warnings"][:10])
        QMessageBox.information(self, "Import complete", message)


class _ReviewPage(QWizardPage):
    def __init__(self):
        super().__init__()
        self.setTitle("Review and confirm")
        self.setSubTitle(
            "Review assignment changes and record the reason for the bulk update."
        )
        layout = QVBoxLayout(self)
        self.summary_label = QLabel()
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(
            ["Room Type", "Added", "Removed", "Quantity changes"]
        )
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents
        )
        for column in range(1, 4):
            self.table.horizontalHeader().setSectionResizeMode(
                column, QHeaderView.Stretch
            )
        layout.addWidget(self.table, 1)
        self.overlap_summary_label = QLabel()
        self.overlap_summary_label.setWordWrap(True)
        layout.addWidget(self.overlap_summary_label)
        self.overlap_resolutions = set()
        self.overlap_table = QTableWidget(0, 5)
        self.overlap_table.setHorizontalHeaderLabels(
            [
                "Room Type",
                "Shared Asset",
                "Contributing Sources",
                "Combined Qty",
                "Resolution",
            ]
        )
        self.overlap_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.overlap_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.overlap_table.verticalHeader().setVisible(False)
        self.overlap_table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents
        )
        self.overlap_table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents
        )
        self.overlap_table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.Stretch
        )
        self.overlap_table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.ResizeToContents
        )
        self.overlap_table.horizontalHeader().setSectionResizeMode(
            4, QHeaderView.ResizeToContents
        )
        self.overlap_table.setMaximumHeight(190)
        layout.addWidget(self.overlap_table)
        self.review_overlaps_button = QPushButton(
            "Review / remove overlaps..."
        )
        self.review_overlaps_button.clicked.connect(self._review_overlaps)
        layout.addWidget(self.review_overlaps_button)
        layout.addWidget(QLabel("Change reason"))
        self.reason_edit = QPlainTextEdit()
        self.reason_edit.setPlaceholderText(
            "Required when assignments or overlap resolutions change"
        )
        self.reason_edit.setMaximumHeight(100)
        layout.addWidget(self.reason_edit)

    def initializePage(self):
        results = self.wizard().build_results()
        self.table.setRowCount(0)
        changed = 0
        for result in results or []:
            before = {
                row["bundle_id"]: int(row["qty"])
                for row in result["before_assignments"]
            }
            after = {
                row["bundle_id"]: int(row["qty"])
                for row in result["assignments"]
            }
            if before == after:
                continue
            changed += 1
            added = [
                f"{bundle_id} × {after[bundle_id]}"
                for bundle_id in after.keys() - before.keys()
            ]
            removed = [
                f"{bundle_id} × {before[bundle_id]}"
                for bundle_id in before.keys() - after.keys()
            ]
            quantities = [
                f"{bundle_id}: {before[bundle_id]} → {after[bundle_id]}"
                for bundle_id in before.keys() & after.keys()
                if before[bundle_id] != after[bundle_id]
            ]
            row = self.table.rowCount()
            self.table.insertRow(row)
            values = [
                f"{result['room_type_id']} - {result['room_type_name']}",
                ", ".join(sorted(added)) or "—",
                ", ".join(sorted(removed)) or "—",
                ", ".join(sorted(quantities)) or "—",
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                self.table.setItem(row, column, item)
        self.summary_label.setText(
            f"{changed} of {len(results or [])} selected room type(s) will change."
            if changed
            else "No assignment changes are currently selected."
        )
        self._refresh_overlaps(results)

    def _refresh_overlaps(self, results):
        overlaps = self.wizard().bundle_overlaps(results)
        available_keys = {
            (int(overlap["room_index"]), overlap["asset_id"])
            for overlap in overlaps
        }
        self.overlap_resolutions.intersection_update(available_keys)
        self.overlap_table.setRowCount(0)
        for overlap in overlaps:
            row = self.overlap_table.rowCount()
            self.overlap_table.insertRow(row)
            asset_id = overlap["asset_id"]
            asset = self.wizard().assets_by_id.get(asset_id, {})
            asset_name = _text(asset.get("name"))
            asset_label = (
                f"{asset_id} - {asset_name}" if asset_name else asset_id
            )
            source_labels = []
            for contribution in overlap["contributions"]:
                if contribution.get("source_type") == "manual":
                    source_labels.append(
                        f"Manually added = {contribution['total_qty']}"
                    )
                else:
                    source_labels.append(
                        f"{contribution['bundle_name']} "
                        f"[{contribution['bundle_id']}] = "
                        f"{contribution['total_qty']}"
                    )
            bundle_text = "; ".join(source_labels)
            values = [
                (
                    f"{overlap['room_type_id']} - "
                    f"{overlap['room_type_name']}"
                ),
                asset_label,
                bundle_text,
                str(overlap["total_qty"]),
                (
                    "Keep one"
                    if (
                        int(overlap["room_index"]),
                        overlap["asset_id"],
                    )
                    in self.overlap_resolutions
                    else "Review"
                ),
            ]
            for column, value in enumerate(values):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                self.overlap_table.setItem(row, column, item)

        if overlaps:
            affected_rooms = {
                overlap["room_type_id"] for overlap in overlaps
            }
            self.overlap_summary_label.setText(
                f"All-room overlap check: {len(overlaps)} overlapping "
                "asset(s) across "
                f"{len(affected_rooms)} room type(s). An asset is listed when "
                "it comes from multiple bundles or from a bundle plus a "
                "manually added room asset. Quantities are added together."
            )
            self.overlap_summary_label.setStyleSheet("color: #9a6700;")
            self.overlap_table.setVisible(True)
            self.review_overlaps_button.setVisible(True)
        else:
            self.overlap_summary_label.setText(
                "All-room overlap check: no assets are shared by multiple "
                "bundles or by a bundle and manually added room assets."
            )
            self.overlap_summary_label.setStyleSheet("color: #287233;")
            self.overlap_table.setVisible(False)
            self.review_overlaps_button.setVisible(False)

    def _review_overlaps(self):
        results = self.wizard().build_results()
        overlaps = self.wizard().bundle_overlaps(results)
        dialog = BundleOverlapReviewDialog(
            self,
            overlaps,
            self.wizard().assets_by_id,
            self.overlap_resolutions,
        )
        if dialog.exec() != QDialog.Accepted:
            return
        self.overlap_resolutions = set(dialog.result_keys)
        self._refresh_overlaps(results)

    def selected_resolutions(self, overlaps):
        return [
            {
                "room_index": int(overlap["room_index"]),
                "room_type_id": overlap["room_type_id"],
                "room_type_name": overlap["room_type_name"],
                "asset_id": overlap["asset_id"],
            }
            for overlap in overlaps
            if (
                int(overlap["room_index"]),
                overlap["asset_id"],
            )
            in self.overlap_resolutions
        ]


class BundleOverlapReviewDialog(QDialog):
    """Choose room-specific overlaps that should be reduced to one asset."""

    def __init__(
        self,
        parent,
        overlaps,
        assets_by_id=None,
        selected_keys=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Review Asset Bundle Overlaps")
        self.resize(1100, 620)
        self.overlaps = list(overlaps or [])
        self.assets_by_id = dict(assets_by_id or {})
        self.result_keys = set(selected_keys or set())

        layout = QVBoxLayout(self)
        explanation = QLabel(
            "Select overlaps to resolve. Applying the change keeps one copy of "
            "the asset in that room type and excludes it from future bundle "
            "recipe updates for that room, so it is not added again."
        )
        explanation.setWordWrap(True)
        layout.addWidget(explanation)

        button_row = QHBoxLayout()
        select_all_button = QPushButton("Select all")
        clear_button = QPushButton("Clear")
        select_all_button.clicked.connect(
            lambda: self._set_all(Qt.Checked)
        )
        clear_button.clicked.connect(
            lambda: self._set_all(Qt.Unchecked)
        )
        button_row.addWidget(select_all_button)
        button_row.addWidget(clear_button)
        button_row.addStretch(1)
        layout.addLayout(button_row)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(
            [
                "Keep one",
                "Room Type",
                "Asset",
                "Contributing Sources",
                "Combined Qty",
            ]
        )
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(
            0, QHeaderView.ResizeToContents
        )
        self.table.horizontalHeader().setSectionResizeMode(
            1, QHeaderView.ResizeToContents
        )
        self.table.horizontalHeader().setSectionResizeMode(
            2, QHeaderView.ResizeToContents
        )
        self.table.horizontalHeader().setSectionResizeMode(
            3, QHeaderView.Stretch
        )
        self.table.horizontalHeader().setSectionResizeMode(
            4, QHeaderView.ResizeToContents
        )
        layout.addWidget(self.table, 1)
        self._populate()

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        buttons.button(QDialogButtonBox.Ok).setText(
            "Use selected resolutions"
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _populate(self):
        for overlap in self.overlaps:
            row = self.table.rowCount()
            self.table.insertRow(row)
            key = (
                int(overlap["room_index"]),
                overlap["asset_id"],
            )
            check_item = QTableWidgetItem("")
            check_item.setFlags(
                Qt.ItemIsEnabled
                | Qt.ItemIsSelectable
                | Qt.ItemIsUserCheckable
            )
            check_item.setCheckState(
                Qt.Checked if key in self.result_keys else Qt.Unchecked
            )
            check_item.setData(Qt.UserRole, key)
            self.table.setItem(row, 0, check_item)

            asset = self.assets_by_id.get(overlap["asset_id"], {})
            asset_name = _text(asset.get("name"))
            asset_label = (
                f"{overlap['asset_id']} - {asset_name}"
                if asset_name
                else overlap["asset_id"]
            )
            source_labels = []
            for contribution in overlap["contributions"]:
                if contribution.get("source_type") == "manual":
                    source_labels.append(
                        f"Manually added = {contribution['total_qty']}"
                    )
                else:
                    source_labels.append(
                        f"{contribution['bundle_name']} "
                        f"[{contribution['bundle_id']}] = "
                        f"{contribution['total_qty']}"
                    )
            values = [
                (
                    f"{overlap['room_type_id']} - "
                    f"{overlap['room_type_name']}"
                ),
                asset_label,
                "; ".join(source_labels),
                str(overlap["total_qty"]),
            ]
            for column, value in enumerate(values, start=1):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                self.table.setItem(row, column, item)

    def _set_all(self, state):
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if item is not None:
                item.setCheckState(state)

    def accept(self):
        self.result_keys = {
            tuple(item.data(Qt.UserRole))
            for row in range(self.table.rowCount())
            if (item := self.table.item(row, 0)) is not None
            and item.checkState() == Qt.Checked
        }
        super().accept()


class RoomBundleAssignmentWizard(QWizard):
    """Select room/bundle scope, edit the matrix, and review changes."""

    def __init__(
        self,
        parent,
        room_types=None,
        bundles=None,
        assets_by_id=None,
    ):
        super().__init__(parent)
        self.setWindowTitle("Assign Asset Bundles to Room Types")
        self.resize(1280, 760)
        self.setWizardStyle(QWizard.ModernStyle)
        self.setOption(QWizard.NoBackButtonOnStartPage, True)
        self.room_types = [
            deepcopy(row) for row in room_types or [] if isinstance(row, dict)
        ]
        self.bundles = normalise_asset_bundles(bundles or [])
        self.assets_by_id = dict(assets_by_id or {})
        self.result = None
        self.selection_page = _SelectionPage(self.room_types, self.bundles)
        self.matrix_page = _MatrixPage()
        self.review_page = _ReviewPage()
        self.addPage(self.selection_page)
        self.addPage(self.matrix_page)
        self.addPage(self.review_page)
        self.setButtonText(QWizard.FinishButton, "Apply assignments")

    def build_results(self):
        quantities = self.matrix_page.quantities(show_errors=False)
        if quantities is None:
            return []
        selected_bundle_ids = {
            _text(bundle.get("id")) for bundle in self.matrix_page.bundles
        }
        valid_bundle_ids = {_text(bundle.get("id")) for bundle in self.bundles}
        results = []
        for source_index, room in self.matrix_page.rooms:
            room_id = _room_id(room, source_index)
            before = clean_bundle_assignments(
                room.get("asset_bundle_assignments", []),
                valid_bundle_ids,
            )
            retained = [
                row
                for row in before
                if row["bundle_id"] not in selected_bundle_ids
            ]
            selected = [
                {"bundle_id": bundle_id, "qty": quantity}
                for bundle_id, quantity in quantities.get(room_id, {}).items()
                if quantity > 0
            ]
            assignments = clean_bundle_assignments(
                [*retained, *selected],
                valid_bundle_ids,
            )
            results.append(
                {
                    "room_index": source_index,
                    "room_type_id": room_id,
                    "room_type_name": _room_name(room),
                    "before_assignments": before,
                    "assignments": assignments,
                }
            )
        return results

    def bundle_overlaps(self, results=None):
        proposed_by_index = {
            int(result.get("room_index", -1)): result
            for result in (
                results if results is not None else self.build_results()
            )
        }
        overlaps = []
        valid_bundle_ids = {
            _text(bundle.get("id")) for bundle in self.bundles
        }
        for room_index, room_type in enumerate(self.room_types):
            proposed = proposed_by_index.get(room_index)
            assignments = (
                proposed.get("assignments", [])
                if proposed is not None
                else clean_bundle_assignments(
                    room_type.get("asset_bundle_assignments", []),
                    valid_bundle_ids,
                )
            )
            for overlap in room_bundle_asset_overlaps(
                room_type,
                assignments,
                self.bundles,
            ):
                overlaps.append(
                    {
                        "room_index": room_index,
                        "room_type_id": _room_id(room_type, room_index),
                        "room_type_name": _room_name(room_type),
                        **overlap,
                    }
                )
        return overlaps

    def accept(self):
        results = self.build_results()
        changed = [
            result
            for result in results
            if result["before_assignments"] != result["assignments"]
        ]
        reason = _text(self.review_page.reason_edit.toPlainText())
        overlaps = self.bundle_overlaps(results)
        resolutions = self.review_page.selected_resolutions(overlaps)
        if (changed or resolutions) and not reason:
            QMessageBox.information(
                self,
                "Assign Asset Bundles",
                "Enter a reason for the room-type bundle assignment or "
                "overlap changes.",
            )
            return
        self.result = {
            "rooms": changed,
            "reason": reason,
            "overlaps": overlaps,
            "overlap_resolutions": resolutions,
        }
        super().accept()
