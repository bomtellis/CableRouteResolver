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

    def __init__(self, parent, incoming, existing, *, asset_label="asset"):
        super().__init__(parent)
        self.incoming = [dict(row) for row in incoming if isinstance(row, dict)]
        self.existing = [dict(row) for row in existing if isinstance(row, dict)]
        self.asset_label = asset_label
        self.resolutions = []
        self._row_controls = []

        self.setWindowTitle("Marshal imported assets")
        self.resize(980, 560)
        layout = QVBoxLayout(self)
        intro = QLabel(
            "Choose how each row is handled. Map keeps an existing local definition, "
            "Create adds the imported definition under the selected ID, and Reject ignores the row."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self.table = QTableWidget(len(self.incoming), 4)
        self.table.setHorizontalHeaderLabels(
            ["Source ID", "Imported name", "Action", "Local asset"]
        )
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.verticalHeader().setVisible(False)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.Stretch)
        layout.addWidget(self.table, 1)

        existing_options = sorted(
            (
                (_text(row.get("id")), _text(row.get("name")))
                for row in self.existing
                if _text(row.get("id"))
            ),
            key=lambda item: (item[1].casefold(), item[0].casefold()),
        )
        existing_ids = {asset_id for asset_id, _name in existing_options}
        reserved = set(existing_ids)
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

            action_combo = QComboBox()
            if existing_options:
                action_combo.addItem("Map to existing", IMPORT_ACTION_MAP)
            action_combo.addItem("Create new", IMPORT_ACTION_CREATE)
            action_combo.addItem("Reject row", IMPORT_ACTION_REJECT)
            default_action = (
                IMPORT_ACTION_MAP if source_id in existing_ids else IMPORT_ACTION_CREATE
            )
            action_combo.setCurrentIndex(action_combo.findData(default_action))
            self.table.setCellWidget(row_index, 2, action_combo)

            target_combo = QComboBox()
            self.table.setCellWidget(row_index, 3, target_combo)
            control = {
                "source_id": source_id,
                "action": action_combo,
                "target": target_combo,
                "existing": existing_options,
                "values": {
                    IMPORT_ACTION_MAP: source_id if source_id in existing_ids else "",
                    IMPORT_ACTION_CREATE: proposals[source_id],
                },
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
            for asset_id, name in control["existing"]:
                label = f"{asset_id} - {name}" if name else asset_id
                target.addItem(label, asset_id)
            selected = control["values"].get(IMPORT_ACTION_MAP, "")
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
                else:
                    created_ids.add(target_id)
            resolutions.append(
                {"source_id": source_id, "action": action, "target_id": target_id}
            )
        if errors:
            QMessageBox.warning(
                self,
                "Resolve asset import",
                "Correct these rows before importing:\n\n" + "\n".join(errors[:12]),
            )
            return
        self.resolutions = resolutions
        self.accept()
