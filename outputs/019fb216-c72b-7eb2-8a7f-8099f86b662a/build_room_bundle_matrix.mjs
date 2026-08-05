import fs from "node:fs/promises";
import path from "node:path";
import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const workDir = path.dirname(new URL(import.meta.url).pathname).replace(
  /^\/([A-Za-z]:)/,
  "$1",
);
const inputPath = path.join(workDir, "room-type-bundle-matrix-input.xlsx");

const input = await FileBlob.load(inputPath);
const workbook = await SpreadsheetFile.importXlsx(input);

const matrixSheet = workbook.worksheets.getItem("Room Bundle Matrix");
const matrixValues = matrixSheet.getRange("A4:AE74").values;
const sourceText = await fs.readFile(
  path.join(workDir, "filled-matrix.tsv"),
  "utf8",
);
const sourceRows = sourceText
  .replace(/^\uFEFF/, "")
  .split(/\r?\n/)
  .filter((line) => line.trim())
  .map((line) => line.split("\t"));

const sourceBundleCodes = sourceRows[0].map((value) => {
  const match = String(value ?? "")
    .trim()
    .replace(/^UCI-4\b/i, "UC1-4")
    .match(/^UC\d+-\d+/i);
  return match ? match[0].toUpperCase() : "";
});

const sourceRowsByName = new Map();
for (let index = 1; index < sourceRows.length; index += 1) {
  const key = String(sourceRows[index][0] ?? "").trim().toLocaleLowerCase();
  if (!sourceRowsByName.has(key)) {
    sourceRowsByName.set(key, []);
  }
  sourceRowsByName.get(key).push(sourceRows[index]);
}

// Authoritative room-name reconciliation, based on the supplied lookup table.
const sourceNamesByTargetId = {
  RT3: ["Standard Room Tech"],
  RT4: ["Touchdown Space"],
  RT6: ["MDT Clinical Support space"],
  RT7: ["Inpatient - Dirty Utility"],
  RT8: ["Clean Utility"],
  RT9: ["Assessment and Treatment room"],
  RT12: ["Triage Rooms"],
  RT13: ["Interview Room"],
  RT14: ["Resus DDB"],
  RT15: ["Consultation Examination Room"],
  RT16: ["CT Scanner room"],
  RT18: ["MRI control room"],
  RT20: ["Ultrasound Room"],
  RT26: ["Reporting: 1 & 3 Person"],
  RT28: ["Cannulation Room"],
  RT29: ["Anaesthetic Room"],
  RT37: ["ICU Single Bedroom Adult"],
  RT38: ["Standard Room"],
  RT39: ["Maternity- Bereavement Lounge"],
  RT40: ["Birthing Pool room"],
  RT41: ["seminar Room ( large & Small)"],
  RT42: ["Procedure Room (Endoscopy Large & Standard)"],
  RT52: ["Fluid Store"],
  RT53: ["Mecdicine Storage and preparation large"],
  RT61: ["Isolation Lobby"],
  RT65: ["Staff Communication Base"],
  RT80: ["Relative overnight Stay"],
  RT81: ["Rehabilitation space"],
  RT85: ["Neonatal-High Dependancy"],
  RT86: ["Single Cit Room"],
  RT88: ["Family Adjourning room"],
  RT98: ["Scrub Up & gowning room"],
  RT118: ["X-ray: General Scanning room and viewing station"],
  RT119: ["Fluroscopy room"],
  RT129: ["Workstation"],
  RT130: ["Command Centre 24 people"],
  RT142: ["Service Room"],
  RT157: ["Cashier/ Patient"],
  RT158: ["Hearing aid Fitting and Repair"],
  RT160: ["Sensory Rooms"],
  RT173: ["Point of care testing room"],
  RT136: ["Bay - Trolley"],
  RT175: ["Interventional Radiology suite"],
  RT177: ["Viewing room (Small & Large)"],
  RT178: ["Inpatient - Clean Supply"],
  RT179: ["Pantry  DDB"],
  RT180: ["Pneumatic Tube Station"],
  RT181: ["Wider Ward Technology"],
  RT182: ["Gowning Lobby"],
  RT183: ["Theatres CheckIn"],
  RT184: ["Block Room"],
  RT185: ["Anaesthetic Stores"],
  RT186: ["On-Call room"],
  RT187: ["Maternity Triage"],
  RT190: ["Family Lounge"],
  RT191: ["Waiting/Play"],
  RT192: ["Play Therapy"],
  RT193: ["School Room"],
  RT194: ["Outpatient Check In"],
  RT195: ["Virtual Consultation Room"],
  RT196: ["Procedure Room (Gynae SDEC EPAU)"],
  RT197: ["Physical Measurement Phlebotomy Room"],
  RT198: ["Command Centre Meeting room"],
  RT199: ["Information Centre"],
  RT200: ["Research Room"],
  RT201: ["Patient Cabin Pre & Post Op"],
};

const targetHeaders = matrixValues[0];
const targetRooms = matrixValues.slice(1);
const targetBundleCodes = targetHeaders.slice(2).map((value) =>
  String(value ?? "").split("|")[0].trim().toUpperCase(),
);
const targetBundleColumn = new Map(
  targetBundleCodes.map((code, index) => [code, index]),
);
const assignmentGrid = targetRooms.map(() =>
  Array(targetBundleCodes.length).fill(null),
);
const unmatchedSourceNames = [];

for (let rowIndex = 0; rowIndex < targetRooms.length; rowIndex += 1) {
  const targetId = String(targetRooms[rowIndex][0] ?? "").trim();
  for (const sourceName of sourceNamesByTargetId[targetId] ?? []) {
    const candidates =
      sourceRowsByName.get(sourceName.toLocaleLowerCase()) ?? [];
    if (!candidates.length) {
      unmatchedSourceNames.push(sourceName);
      continue;
    }
    // When the filled matrix repeats a room name, use the populated occurrence.
    const sourceRow = [...candidates].sort(
      (left, right) =>
        right.filter((value, index) => index > 0 && String(value ?? "").trim())
          .length -
        left.filter((value, index) => index > 0 && String(value ?? "").trim())
          .length,
    )[0];
    for (let column = 1; column < sourceRow.length; column += 1) {
      if (!String(sourceRow[column] ?? "").trim()) {
        continue;
      }
      const targetColumn = targetBundleColumn.get(sourceBundleCodes[column]);
      if (targetColumn !== undefined) {
        assignmentGrid[rowIndex][targetColumn] = 1;
      }
    }
  }
}

if (unmatchedSourceNames.length) {
  throw new Error(
    `Mapped source room names were not found: ${unmatchedSourceNames.join(", ")}`,
  );
}

matrixSheet.getRange("C5:AE74").values = assignmentGrid;
matrixSheet.getRange("C5:AE74").format.numberFormat = "0";

const assignmentRows = [];
for (let rowIndex = 0; rowIndex < targetRooms.length; rowIndex += 1) {
  const [roomId, roomName] = targetRooms[rowIndex];
  for (
    let bundleIndex = 0;
    bundleIndex < targetBundleCodes.length;
    bundleIndex += 1
  ) {
    if (assignmentGrid[rowIndex][bundleIndex] !== 1) {
      continue;
    }
    const bundleId = targetBundleCodes[bundleIndex];
    assignmentRows.push([roomId, roomName, bundleId, bundleId, 1]);
  }
}

const assignmentSheet = workbook.worksheets.getItem("Assignment Rows");
if (assignmentRows.length) {
  assignmentSheet
    .getRangeByIndexes(1, 0, assignmentRows.length, 5)
    .values = assignmentRows;
  assignmentSheet
    .getRangeByIndexes(1, 4, assignmentRows.length, 1)
    .format.numberFormat = "0";
}

const matchedTargetIds = Object.keys(sourceNamesByTargetId);
const unmatchedTargetRooms = targetRooms
  .filter((row) => !sourceNamesByTargetId[String(row[0] ?? "").trim()])
  .map((row) => ({ id: row[0], name: row[1] }));

const matrixCheck = await workbook.inspect({
  kind: "table",
  range: "Room Bundle Matrix!A4:AE12",
  include: "values,formulas",
  tableMaxRows: 9,
  tableMaxCols: 31,
  maxChars: 10000,
});
console.log(matrixCheck.ndjson);
const rowCheck = await workbook.inspect({
  kind: "table",
  range: `Assignment Rows!A1:E${Math.min(assignmentRows.length + 1, 21)}`,
  include: "values,formulas",
  tableMaxRows: 21,
  tableMaxCols: 5,
  maxChars: 10000,
});
console.log(rowCheck.ndjson);
const formulaErrors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 300 },
  summary: "final formula error scan",
});
console.log(formulaErrors.ndjson);

const outputPath = path.join(workDir, "room-type-bundle-matrix-matched.xlsx");
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);

const savedWorkbook = await SpreadsheetFile.importXlsx(
  await FileBlob.load(outputPath),
);
const savedMatrix = savedWorkbook
  .worksheets
  .getItem("Room Bundle Matrix")
  .getRange("C5:AE74").values;
const savedAssignmentCount = savedMatrix
  .flat()
  .filter((value) => Number(value) === 1).length;
const savedAssignmentRows = savedWorkbook
  .worksheets
  .getItem("Assignment Rows")
  .getUsedRange(true).values;
if (
  savedAssignmentCount !== assignmentRows.length ||
  savedAssignmentRows.length !== assignmentRows.length + 1
) {
  throw new Error(
    `Round-trip verification failed: matrix=${savedAssignmentCount}, ` +
      `rows=${savedAssignmentRows.length - 1}, expected=${assignmentRows.length}`,
  );
}

for (const sheetName of [
  "Room Bundle Matrix",
  "Assignment Rows",
  "Reference",
]) {
  try {
    const preview = await workbook.render({
      sheetName,
      autoCrop: "all",
      scale: 1,
      format: "png",
    });
    await fs.writeFile(
      path.join(workDir, `${sheetName.replaceAll(" ", "-")}-after.png`),
      new Uint8Array(await preview.arrayBuffer()),
    );
  } catch (error) {
    console.log(`Render skipped for ${sheetName}: ${error.message}`);
  }
}

console.log(
  JSON.stringify({
    outputPath,
    matchedTargetRooms: matchedTargetIds.length,
    unmatchedTargetRooms,
    assignmentCount: assignmentRows.length,
  }),
);
