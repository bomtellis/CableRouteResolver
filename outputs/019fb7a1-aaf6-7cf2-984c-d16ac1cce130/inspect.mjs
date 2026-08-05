import fs from "node:fs/promises";
import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const base = "C:/Users/tombe/Documents/CableRouteResolver/outputs/019fb7a1-aaf6-7cf2-984c-d16ac1cce130";
const input = await FileBlob.load(`${base}/source.xlsx`);
const workbook = await SpreadsheetFile.importXlsx(input);

const summary = await workbook.inspect({
  kind: "workbook,sheet,table,drawing,definedName",
  maxChars: 12000,
  tableMaxRows: 12,
  tableMaxCols: 12,
  tableMaxCellChars: 100,
});
console.log("SUMMARY");
console.log(summary.ndjson);

const sheetInfo = await workbook.inspect({ kind: "sheet", include: "id,name", maxChars: 4000 });
console.log("SHEETS");
console.log(sheetInfo.ndjson);

for (let i = 0; i < workbook.worksheets.items.length; i += 1) {
  const sheet = workbook.worksheets.getItemAt(i);
  const used = sheet.getUsedRange();
  console.log(`SHEET ${i}: ${sheet.name} USED ${used?.address ?? "none"}`);
  if (used) {
    const region = await workbook.inspect({
      kind: "region",
      sheetId: sheet.name,
      range: used.address,
      maxChars: 16000,
      tableMaxRows: 80,
      tableMaxCols: 20,
      tableMaxCellChars: 120,
    });
    console.log(region.ndjson);
    const formulas = await workbook.inspect({
      kind: "formula",
      sheetId: sheet.name,
      range: used.address,
      maxChars: 12000,
      options: { maxResults: 300 },
    });
    console.log("FORMULAS");
    console.log(formulas.ndjson);
    const styles = await workbook.inspect({
      kind: "computedStyle",
      sheetId: sheet.name,
      range: used.address,
      maxChars: 8000,
    });
    console.log("STYLES");
    console.log(styles.ndjson);
  }
  const preview = await workbook.render({
    sheetName: sheet.name,
    autoCrop: "all",
    scale: 1.5,
    format: "png",
  });
  await fs.writeFile(`${base}/before_${i}_${sheet.name.replaceAll(/[^A-Za-z0-9_-]/g, "_")}.png`, new Uint8Array(await preview.arrayBuffer()));
}

for (const [name, rangeAddress] of [
  ["Category Model", "A1:AE20"],
  ["Decision Guide", "A1:F15"],
  ["Assumptions", "A1:F21"],
]) {
  const sheet = workbook.worksheets.getItem(name);
  const range = sheet.getRange(rangeAddress);
  console.log(`FOCUSED ${name} ${rangeAddress}`);
  console.log(JSON.stringify({ values: range.values, formulas: range.formulas }, null, 2));
  range.values.forEach((row, index) => console.log(`ROW ${name} ${index + 1}: ${JSON.stringify(row)}`));
  range.formulas.forEach((row, index) => console.log(`FROW ${name} ${index + 1}: ${JSON.stringify(row)}`));
}
