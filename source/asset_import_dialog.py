"""Interactive per-row marshalling for asset-pack imports."""

from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHeaderView,
    QLabel,
    QMessageBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from asset_library_io import (
    IMPORT_ACTION_CREATE,
    IMPORT_ACTION_MAP,
    IMPORT_ACTION_REJECT,
)


def _text(value) -> str:
    return str(value if value is not None else "").strip()


def _adb_code(row) -> str:
    if not isinstance(row, dict):
        return ""
    return _text(row.get("ADB_Code") or row.get("adb_code", ""))


def _existing_asset_match(source, existing) -> dict:
    """Suggest an existing project asset using ID and ADB-code matches."""
    source_id = _text(source.get("id")) if isinstance(source, dict) else ""
    source_adb = _adb_code(source)
    source_adb_key = source_adb.casefold()
    existing_rows = [
        row
        for row in existing
        if isinstance(row, dict) and _text(row.get("id"))
    ]
    existing_by_id = {
        _text(row.get("id")): row
        for row in existing_rows
    }
    adb_matches = [
        row
        for row in existing_rows
        if source_adb_key and _adb_code(row).casefold() == source_adb_key
    ]
    adb_match_ids = [_text(row.get("id")) for row in adb_matches]
    id_match = existing_by_id.get(source_id)

    if id_match is not None:
        existing_adb = _adb_code(id_match)
        conflicting_adb_ids = [
            asset_id for asset_id in adb_match_ids if asset_id != source_id
        ]
        id_adb_conflict = bool(
            source_adb
            and existing_adb
            and source_adb.casefold() != existing_adb.casefold()
        )
        if conflicting_adb_ids or id_adb_conflict:
            details = [f"ID matches {source_id}"]
            if conflicting_adb_ids:
                details.append(
                    f"ADB {source_adb} matches {', '.join(conflicting_adb_ids)}"
                )
            elif id_adb_conflict:
                details.append(
                    f"existing ADB is {existing_adb}"
                )
            return {
                "target_id": "",
                "status": "Conflict: " + "; ".join(details),
                "requires_choice": True,
            }
        status = f"Matched ID and ADB {source_adb}" if source_adb else "Matched ID"
        return {
            "target_id": source_id,
            "status": status,
            "requires_choice": False,
        }

    if len(adb_matches) == 1:
        target_id = adb_match_ids[0]
        return {
            "target_id": target_id,
            "status": f"Matched ADB {source_adb} to {target_id}",
            "requires_choice": False,
        }
    if len(adb_matches) > 1:
        return {
            "target_id": "",
            "status": (
                f"ADB {source_adb} matches {len(adb_matches)} existing assets"
            ),
            "requires_choice": True,
        }
    return {
        "target_id": "",
        "status": "No existing ID or ADB match",
        "requires_choice": False,
    }


def _new_id_proposal(source_id: str, reserved: set[str]) -> str:
    if source_id and source_id not in reserved:
        return source_id
    base = f"{source_id or 'ASSET'}_imported"
    candidate = base
    suffix = 2
    while candidate in reserved:
        candidate = f"{base}_{suffix}"
        suffix += 1
    return candidate


class AssetImportMarshallingDialog(QDialog):
    """Resolve every imported definition to an existing, new, or rejected row."""

    def __init__(
        self,
        parent,
        incoming,
        existing,
        *,
        asset_label="asset",
        reserved_ids=None,
        match_adb_codes=False,
    ):
        super().__init__(parent)
        self.incoming = [dict(row) for row in incoming if isinstance(row, dict)]
        self.existing = [dict(row) for row in existing if isinstance(row, dict)]
        self.asset_label = asset_label
        self.match_adb_codes = bool(match_adb_codes)
        self.asset_label_plural = (
            "assets" if self.asset_label == "asset" else f"{self.asset_label}s"
        )
        self.reserved_ids = {
            _text(asset_id) for asset_id in (reserved_ids or []) if _text(asset_id)
        }
        self.resolutions = []
        self._row_controls = []

        self.setWindowTitle(f"Marshal imported {self.asset_label_plural}")
        self.resize(1180 if self.match_adb_codes else 980, 560)
        layout = QVBoxLayout(self)
        intro = QLabel(
            "Choose how each row is handled. Map keeps an existing local definition, "
            "Create adds the imported definition under the selected ID, and Reject ignores the row."
            + (
                " Matching checks both asset ID and ADB code."
                if self.match_adb_codes
                else ""
            )
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self._action_column = 4 if self.match_adb_codes else 2
        self._target_column = 5 if self.match_adb_codes else 3
        self.table = QTableWidget(
            len(self.incoming),
            6 if self.match_adb_codes else 4,
        )
        self.table.setHorizontalHeaderLabels(
            (
                [
                    "Source ID",
                    "Imported name",
                    "ADB code",
                    "Existing match",
                    "Action",
                    f"Local {self.asset_label}",
                ]
                if self.match_adb_codes
                else [
                    "Source ID",
                    "Imported name",
                    "Action",
                    f"Local {self.asset_label}",
                ]
            )
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        if self.match_adb_codes:
            header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
            header.setSectionResizeMode(3, QHeaderView.Stretch)
        header.setSectionResizeMode(
            self._action_column, QHeaderView.ResizeToContents
        )
        header.setSectionResizeMode(self._target_column, QHeaderView.Stretch)
        layout.addWidget(self.table, 1)

        existing_options = sorted(
            (
                (
                    _text(row.get("id")),
                    _text(row.get("name")),
                    _adb_code(row),
                )
                for row in self.existing
                if _text(row.get("id"))
            ),
            key=lambda item: (item[1].casefold(), item[0].casefold()),
        )
        existing_ids = {
            asset_id for asset_id, _name, _adb in existing_options
        }
        reserved = set(existing_ids) | self.reserved_ids
        proposals = {}
        for row in self.incoming:
            source_id = _text(row.get("id"))
            proposal = _new_id_proposal(source_id, reserved)
            proposals[source_id] = proposal
            reserved.add(proposal)

        for row_index, source in enumerate(self.incoming):
            source_id = _text(source.get("id"))
            source_item = QTableWidgetItem(source_id)
            source_item.setData(Qt.UserRole, source_id)
            self.table.setItem(row_index, 0, source_item)
            self.table.setItem(row_index, 1, QTableWidgetItem(_text(source.get("name"))))

            match = (
                _existing_asset_match(source, self.existing)
                if self.match_adb_codes
                else {
                    "target_id": source_id if source_id in existing_ids else "",
                    "status": "",
                    "requires_choice": False,
                }
            )
            if self.match_adb_codes:
                self.table.setItem(
                    row_index, 2, QTableWidgetItem(_adb_code(source))
                )
                self.table.setItem(
                    row_index, 3, QTableWidgetItem(match["status"])
                )

            action_combo = QComboBox()
            if existing_options:
                action_combo.addItem("Map to existing", IMPORT_ACTION_MAP)
            action_combo.addItem("Create new", IMPORT_ACTION_CREATE)
            action_combo.addItem("Reject row", IMPORT_ACTION_REJECT)
            default_action = (
                IMPORT_ACTION_MAP
                if match["target_id"] or match["requires_choice"]
                else IMPORT_ACTION_CREATE
            )
            action_combo.setCurrentIndex(action_combo.findData(default_action))
            self.table.setCellWidget(
                row_index, self._action_column, action_combo
            )

            target_combo = QComboBox()
            self.table.setCellWidget(
                row_index, self._target_column, target_combo
            )
            control = {
                "source_id": source_id,
                "action": action_combo,
                "target": target_combo,
                "existing": existing_options,
                "values": {
                    IMPORT_ACTION_MAP: match["target_id"],
                    IMPORT_ACTION_CREATE: proposals[source_id],
                },
                "requires_explicit_map_target": match["requires_choice"],
                "current_action": "",
            }
            self._row_controls.append(control)
            action_combo.currentIndexChanged.connect(
                lambda _index, current=control: self._configure_target(current)
            )
            self._configure_target(control)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.button(QDialogButtonBox.Ok).setText("Import resolved rows")
        buttons.accepted.connect(self._accept_resolutions)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _configure_target(self, control) -> None:
        old_action = control["current_action"]
        target = control["target"]
        if old_action == IMPORT_ACTION_MAP:
            control["values"][old_action] = _text(target.currentData())
        elif old_action == IMPORT_ACTION_CREATE:
            control["values"][old_action] = _text(target.currentText())

        action = _text(control["action"].currentData())
        control["current_action"] = action
        target.blockSignals(True)
        target.clear()
        if action == IMPORT_ACTION_MAP:
            target.setEnabled(True)
            target.setEditable(False)
            selected = control["values"].get(IMPORT_ACTION_MAP, "")
            if control["requires_explicit_map_target"] and not selected:
                target.addItem("Choose an existing asset...", "")
            for asset_id, name, adb_code in control["existing"]:
                label = f"{asset_id} - {name}" if name else asset_id
                if self.match_adb_codes and adb_code:
                    label += f" [ADB: {adb_code}]"
                target.addItem(label, asset_id)
            index = target.findData(selected)
            target.setCurrentIndex(index if index >= 0 else 0)
        elif action == IMPORT_ACTION_CREATE:
            target.setEnabled(True)
            target.setEditable(True)
            target.addItem(control["values"].get(IMPORT_ACTION_CREATE, ""))
            target.setEditText(control["values"].get(IMPORT_ACTION_CREATE, ""))
        else:
            target.setEditable(False)
            target.setEnabled(False)
            target.addItem("Row will not be imported")
        target.blockSignals(False)

    def _target_id(self, control, action: str) -> str:
        target = control["target"]
        if action == IMPORT_ACTION_MAP:
            return _text(target.currentData())
        if action == IMPORT_ACTION_CREATE:
            return _text(target.currentText())
        return ""

    def _accept_resolutions(self) -> None:
        existing_ids = {
            _text(row.get("id")) for row in self.existing if _text(row.get("id"))
        }
        created_ids = set()
        resolutions = []
        errors = []
        for control in self._row_controls:
            source_id = control["source_id"]
            action = _text(control["action"].currentData())
            target_id = self._target_id(control, action)
            if action == IMPORT_ACTION_MAP and target_id not in existing_ids:
                errors.append(f"{source_id}: choose an existing {self.asset_label}.")
            elif action == IMPORT_ACTION_CREATE:
                if not target_id:
                    errors.append(f"{source_id}: enter an ID for the new {self.asset_label}.")
                elif target_id in existing_ids or target_id in created_ids:
                    errors.append(f"{source_id}: ID {target_id} is already in use.")
                elif target_id in self.reserved_ids:
                    errors.append(
                        f"{source_id}: ID {target_id} was previously used and is retired."
                    )
                else:
                    created_ids.add(target_id)
            resolutions.append(
                {"source_id": source_id, "action": action, "target_id": target_id}
            )
        if errors:
            QMessageBox.warning(
                self,
                f"Resolve {self.asset_label} import",
                "Correct these rows before importing:\n\n" + "\n".join(errors[:12]),
            )
            return
        self.resolutions = resolutions
        self.accept()
