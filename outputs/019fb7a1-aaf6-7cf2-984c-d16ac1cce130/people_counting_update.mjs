import fs from "node:fs/promises";
import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const base = "C:/Users/tombe/Documents/CableRouteResolver/outputs/019fb7a1-aaf6-7cf2-984c-d16ac1cce130";
const inputPath = `${base}/LoRaWAN_vs_NB-IoT_PoE_5yr_TCO2.xlsx`;
const outputPath = `${base}/LoRaWAN_vs_NB-IoT_PoE_5yr_TCO_people_counting.xlsx`;
const input = await FileBlob.load(inputPath);
const workbook = await SpreadsheetFile.importXlsx(input);
workbook.comments.setSelf({ displayName: "User" });

const assumptions = workbook.worksheets.getItem("Assumptions");
assumptions.deleteAllDrawings();

const category = workbook.worksheets.getItem("Category Model");
category.getRange("A21:AE21").copyFrom(category.getRange("A20:AE20"), "all");
category.getRange("A21:O21").values = [[
  "People counting / footfall",
  "Entrances / circulation",
  null,
  1,
  220,
  250,
  120,
  3,
  3,
  3,
  4,
  4,
  "Transmit aggregate counts and occupancy metadata only; images or video require local processing and higher-bandwidth backhaul",
  "PoE for camera/ToF analytics or continuous counts; LPWAN only for privacy-preserving aggregate count metadata",
  "Medium",
]];
category.getRange("P21:AE21").formulas = [[
  "=C21*(E21+G21+'Assumptions'!$B$13)",
  "=D21*'Assumptions'!$B$6*('Assumptions'!$B$7+'Assumptions'!$B$8)+'Assumptions'!$B$5*(D21*'Assumptions'!$B$6*('Assumptions'!$B$9+'Assumptions'!$B$10)+'Assumptions'!$B$12)",
  "=C21*E21*'Assumptions'!$B$19/100*'Assumptions'!$B$5+C21*IF(I21<='Assumptions'!$B$5,'Assumptions'!$B$20,0)",
  "=C21*'Assumptions'!$B$11*12*'Assumptions'!$B$5",
  '=IF(COUNT(C21)=0,"",SUM(P21:S21))',
  '=IF(COUNT(C21)=0,"",ROUND(T21/C21,2))',
  "=IF(COUNT(C21)=0,\"\",C21*(F21+G21+'Assumptions'!$B$18+'Assumptions'!$B$15+'Assumptions'!$B$14*12*'Assumptions'!$B$5))",
  "=IF(COUNT(C21)=0,\"\",ROUND(C21*MAX(0,H21-'Assumptions'!$B$16)*12*'Assumptions'!$B$5*'Assumptions'!$B$17,2))",
  "=IF(COUNT(C21)=0,\"\",C21*F21*'Assumptions'!$B$19/100*'Assumptions'!$B$5+C21*IF(J21<='Assumptions'!$B$5,'Assumptions'!$B$20,0))",
  '=IF(COUNT(C21)=0,"",SUM(V21:X21))',
  '=IF(COUNT(C21)=0,"",ROUND(Y21/C21,2))',
  '=IF(COUNT(C21)=0,"",ROUND(T21-Y21,2))',
  '=IF(COUNT(C21)=0,"Not modelled",IF(OR(K21<=2,L21<=2),"Not decisive",IF(T21<Y21,"LoRaWAN",IF(T21>Y21,"NB-IoT","Tie"))))',
  '=IF(COUNT(C21)=0,"",ABS(T21-Y21)/MAX(T21,Y21))',
  '=IF(COUNT(C21)=0,"",IF((Z21-U21+Q21/C21)<=0,"N/A",ROUND((Q21/D21)/(Z21-U21+Q21/C21),0)))',
  '=IF(COUNT(C21)=0,"Not modelled",IF(OR(K21<=2,L21<=2),"Technical constraint dominates cost",IF(AB21=N21,"Cost and architecture align","Review coverage, power and support model")))',
]];
category.getRange("A21:O21").format.rowHeight = 42;
category.getRange("C21:L21").format.fill = "#DDEBF7";
category.getRange("C21:L21").format.font = { color: "#0000FF" };
category.getRange("P21:AE21").format.font = { color: "#000000" };
category.getRange("E21:G21").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
category.getRange("H21:J21").setNumberFormat("0.0");
category.getRange("K21:L21").setNumberFormat("0");
category.getRange("P21:AA21").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
category.getRange("AC21").setNumberFormat("0.0%");
workbook.comments.addThread(
  { cell: category.getRange("E21") },
  "Planning allowance for a privacy-preserving people-counting endpoint. Replace with the selected thermal, time-of-flight, radar or edge-analytics device quotation.",
);

const poe = workbook.worksheets.getItem("PoE Model");
poe.getRange("A21:P21").copyFrom(poe.getRange("A20:P20"), "all");
poe.getRange("A21:D21").formulas = [[
  "='Category Model'!A21",
  "='Category Model'!B21",
  "='Category Model'!C21",
  "='Category Model'!D21",
]];
poe.getRange("E21:K21").values = [[
  450,
  250,
  12,
  5,
  "Continuous power and Ethernet suit overhead ToF, stereo or edge-video counters; avoid exporting identifiable imagery unless necessary",
  "PoE is preferred for continuous people counting and local edge analytics; configure aggregate outputs and privacy controls",
  "High",
]];
poe.getRange("L21:P21").formulas = [[
  "=IF(COUNT('Category Model'!C21)=0,\"\",C21*(E21+F21+'Assumptions'!$B$25))",
  "=IF(COUNT('Category Model'!C21)=0,\"\",D21*('Assumptions'!$B$22+'Assumptions'!$B$23))",
  "=IF(COUNT('Category Model'!C21)=0,\"\",ROUND(C21*G21/1000*8760*'Assumptions'!$B$26/'Assumptions'!$B$27*'Assumptions'!$B$5+D21*'Assumptions'!$B$24*'Assumptions'!$B$5+C21*E21*'Assumptions'!$B$28/100*'Assumptions'!$B$5,2))",
  '=IF(COUNT(\'Category Model\'!C21)=0,"",ROUND(SUM(L21:N21),2))',
  '=IF(COUNT(\'Category Model\'!C21)=0,"",ROUND(O21/C21,2))',
]];
poe.getRange("A21:P21").format.rowHeight = 48;
poe.getRange("A21:D21").format.font = { color: "#008000" };
poe.getRange("E21:K21").format = { fill: "#DDEBF7", font: { color: "#0000FF" }, wrapText: true };
poe.getRange("L21:P21").format.font = { color: "#000000" };
poe.getRange("E21:F21").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
poe.getRange("G21").setNumberFormat("0.0");
poe.getRange("H21").setNumberFormat("0");
poe.getRange("L21:P21").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");

const summary = workbook.worksheets.getItem("Summary");
summary.getRange("B5:B9").formulas = [
  ["=COUNTA('Category Model'!A5:A21)"],
  ["=COUNT('Category Model'!C5:C21)"],
  ['=COUNTIF(H13:H29,"LoRaWAN")'],
  ['=COUNTIF(H13:H29,"NB-IoT")'],
  ['=COUNTIF(H13:H29,"PoE")'],
];
summary.getRange("D5").values = [[
  "PoE is modelled as a third architecture, including endpoint hardware, cabling and installation, shared switch/network infrastructure, electricity, support and maintenance. People counting is now included: PoE is the natural fit for continuous ToF/camera/edge analytics, while LPWAN options are suitable only when endpoints transmit privacy-preserving aggregate counts. Blank device quantities remain deliberately unconfigured; enter units and sites in Category Model and replace blue assumptions with surveyed costs.",
]];
summary.getRange("A14:J14").copyFrom(summary.getRange("A13:J13"), "all");
summary.getRange("A14:G14").formulas = [[
  "='Category Model'!A21",
  "='Category Model'!K21",
  "='Category Model'!L21",
  "='PoE Model'!H21",
  "='Category Model'!U21",
  "='Category Model'!Z21",
  "='PoE Model'!P21",
]];
summary.getRange("H14").formulas = [[
  '=IF(COUNT(\'Category Model\'!C21)=0,"Not modelled",IF(MAX(B14:D14)<=2,"Technical constraint",IF(IF(B14>=3,E14,1E+99)=MIN(IF(B14>=3,E14,1E+99),IF(C14>=3,F14,1E+99),IF(D14>=3,G14,1E+99)),"LoRaWAN",IF(IF(C14>=3,F14,1E+99)=MIN(IF(B14>=3,E14,1E+99),IF(C14>=3,F14,1E+99),IF(D14>=3,G14,1E+99)),"NB-IoT","PoE"))))',
]];
summary.getRange("I14").formulas = [[
  '=IF(H14="PoE",\'PoE Model\'!J21,IF(H14="Not modelled","Enter device quantity in Category Model",\'Category Model\'!N21))',
]];
summary.getRange("J14").formulas = [["=IF(H14=\"PoE\",'PoE Model'!K21,'Category Model'!O21)"]];
summary.getRange("A14:G14").format.font = { color: "#008000" };
summary.getRange("A14").format.font = { bold: true, color: "#008000" };
summary.getRange("H14:J14").format.font = { color: "#000000" };
summary.getRange("I14").format.fill = "#D9F0ED";
summary.getRange("E14:G14").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
summary.getRange("T12:Z29").clear({ applyTo: "all" });
summary.getRange("W12:Z12").values = [["Sensor / use case", "LoRaWAN", "NB-IoT", "PoE"]];
for (let r = 13; r <= 29; r += 1) {
  summary.getRange(`W${r}:Z${r}`).formulas = [[`=A${r}`, `=E${r}`, `=F${r}`, `=G${r}`]];
}
summary.charts.deleteAll();
const chart = summary.charts.add("bar", summary.getRange("W12:Z29"));
chart.title = "Five-year TCO per device by category (£)";
chart.hasLegend = true;
chart.yAxis = { numberFormatCode: "£#,##0" };
chart.setPosition("L4", "U29");

const guide = workbook.worksheets.getItem("Decision Guide");
guide.getRange("A15:G15").unmerge();
guide.getRange("A16:G16").copyFrom(guide.getRange("A15:G15"), "all");
guide.getRange("A16:G16").merge();
guide.getRange("A16").values = [["Suitability score: 5 = natural fit; 4 = good fit; 3 = conditional; 2 = major constraint; 1 = unsuitable for the stated behaviour."]];
guide.getRange("A15:G15").copyFrom(guide.getRange("A14:G14"), "all");
guide.getRange("A15:G15").values = [[
  "Does people counting use images or potentially identifiable data?",
  "Only aggregate counts",
  "Only aggregate counts",
  "Strong with edge processing",
  "PoE with privacy-by-design controls",
  "Accuracy, retention and privacy requirements may dominate connectivity cost",
  "Prefer anonymous ToF/thermal/radar or edge aggregation; assess transparency, retention and DPIA needs",
]];
guide.getRange("D15").format.fill = "#EDE9FE";
guide.getRange("A15:G15").format.rowHeight = 52;
guide.getRange("A16:G16").format.rowHeight = 34;

const sources = workbook.worksheets.getItem("Sources");
sources.getRange("A21:F21").copyFrom(sources.getRange("A20:F20"), "all");
sources.getRange("A21:F21").values = [[
  "People counting privacy",
  "Video surveillance that processes personal data requires data protection by design, fairness, accountability and transparency",
  "https://ico.org.uk/for-organisations/uk-gdpr-guidance-and-resources/cctv-and-video-surveillance/guidance-on-video-surveillance-including-cctv/about-this-guidance/",
  new Date(2026, 6, 31),
  "UK regulator",
  "People-counting decision boundary and privacy-by-design recommendation",
]];
sources.getRange("D21").setNumberFormat("dd mmm yyyy");
sources.getRange("A21:F21").format.wrapText = true;
sources.getRange("A21:F21").format.rowHeight = 72;

const checks = workbook.worksheets.getItem("Checks");
checks.getRange("B5:E8").formulas = [
  ["=SUM('PoE Model'!O5:O21)-SUM('PoE Model'!L5:N21)", "=0", "=B5-C5", "=0.01"],
  ["=COUNT('PoE Model'!P5:P21)", "=COUNT('Category Model'!C5:C21)", "=B6-C6", "=0"],
  ["=MIN('Assumptions'!B22:B28,'PoE Model'!E5:H21)", "=0", "=MIN(0,B7-C7)", "=0"],
  ["='Summary'!B5", "=COUNTA('Category Model'!A5:A21)", "=B8-C8", "=0"],
];

const summaryCheck = await workbook.inspect({
  kind: "table",
  range: "Summary!A4:J29",
  include: "values,formulas",
  tableMaxRows: 30,
  tableMaxCols: 12,
  maxChars: 14000,
});
console.log("SUMMARY CHECK");
console.log(summaryCheck.ndjson);

const categoryCheck = await workbook.inspect({
  kind: "table",
  range: "'Category Model'!A20:AE21",
  include: "values,formulas",
  tableMaxRows: 4,
  tableMaxCols: 32,
  maxChars: 10000,
});
console.log("CATEGORY CHECK");
console.log(categoryCheck.ndjson);

const poeCheck = await workbook.inspect({
  kind: "table",
  range: "'PoE Model'!A20:P21",
  include: "values,formulas",
  tableMaxRows: 4,
  tableMaxCols: 18,
  maxChars: 8000,
});
console.log("POE CHECK");
console.log(poeCheck.ndjson);

const externalRefScan = await workbook.inspect({
  kind: "match",
  searchTerm: "\\[1\\]",
  options: { useRegex: true, maxResults: 100 },
  summary: "unexpected external-reference scan",
});
console.log("EXTERNAL REF SCAN");
console.log(externalRefScan.ndjson);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 300 },
  summary: "final formula error scan",
});
console.log("ERROR SCAN");
console.log(errors.ndjson);

for (let i = 0; i < workbook.worksheets.items.length; i += 1) {
  const sheet = workbook.worksheets.getItemAt(i);
  const preview = await workbook.render({
    sheetName: sheet.name,
    autoCrop: "all",
    scale: 1.25,
    format: "png",
  });
  await fs.writeFile(
    `${base}/people_after_${i}_${sheet.name.replaceAll(/[^A-Za-z0-9_-]/g, "_")}.png`,
    new Uint8Array(await preview.arrayBuffer()),
  );
}

const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);
console.log(`OUTPUT ${outputPath}`);
