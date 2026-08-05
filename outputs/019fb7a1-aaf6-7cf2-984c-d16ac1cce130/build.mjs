import fs from "node:fs/promises";
import { FileBlob, SpreadsheetFile } from "@oai/artifact-tool";

const base = "C:/Users/tombe/Documents/CableRouteResolver/outputs/019fb7a1-aaf6-7cf2-984c-d16ac1cce130";
const sourcePath = `${base}/source.xlsx`;
const outputPath = `${base}/LoRaWAN_vs_NB-IoT_PoE_5yr_TCO.xlsx`;

const palette = {
  navy: "#17324D",
  teal: "#0F766E",
  pale: "#E7EDF3",
  line: "#B9C3CE",
  input: "#DDEBF7",
  blue: "#0000FF",
  green: "#008000",
  white: "#FFFFFF",
  black: "#000000",
  note: "#FFF2CC",
  noteText: "#664D03",
  poe: "#7C3AED",
  softPoe: "#EDE9FE",
  ok: "#C6EFCE",
  okText: "#006100",
  fail: "#FFC7CE",
  failText: "#9C0006",
};

const input = await FileBlob.load(sourcePath);
const workbook = await SpreadsheetFile.importXlsx(input);
workbook.comments.setSelf({ displayName: "User" });

const assumptions = workbook.worksheets.getItem("Assumptions");
assumptions.getRange("A1").values = [["LoRaWAN vs NB-IoT vs PoE — Planning Assumptions"]];
assumptions.getRange("A2").values = [["Blue cells are editable. Costs exclude VAT and the business application/dashboard layer common to all options."]];
assumptions.getRange("A22:F28").values = [
  ["PoE switch/network hardware", 1200, "£/site", "Planning allowance", null, "Use only incremental switch/UPS capacity attributable to this deployment"],
  ["PoE switch/network installation", 500, "£/site", "Planning allowance", null, "Rack, patching, configuration and acceptance; excludes major comms-room works"],
  ["PoE switch/network support", 120, "£/site/year", "Planning allowance", null, "Monitoring, firmware and replacement allowance"],
  ["PoE endpoint commissioning", 20, "£/device", "Planning allowance", null, "Port configuration, labelling, test and handover"],
  ["Electricity price", 0.31, "£/kWh", "Planning allowance", null, "Update to the organisation's delivered electricity rate"],
  ["PoE power delivery efficiency", 0.9, "%", "Planning allowance", "https://ethernetalliance.org/wp-content/uploads/2019/12/WP_EA_Overview8023bt_V2p1_FINAL.pdf", "Use measured PSE/cabling efficiency where available"],
  ["PoE endpoint maintenance", 1, "% of hardware/year", "Planning allowance", null, "Endpoint-only allowance; switch support is modelled separately"],
];
assumptions.getRange("A22:F28").format = {
  font: { typeface: "Carlito", fontSize: 10, color: palette.black },
  wrapText: true,
  borders: { insideHorizontal: { style: "thin", color: palette.line } },
};
assumptions.getRange("B22:B28").format = {
  fill: palette.input,
  font: { color: palette.blue },
};
assumptions.getRange("B22:B26").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
assumptions.getRange("B27").setNumberFormat("0.0%");
assumptions.getRange("B28").setNumberFormat("0.0");
assumptions.getRange("A22:A28").format.font = { bold: false, color: palette.black };
assumptions.getRange("A22:F28").format.rowHeight = 34;
workbook.comments.addThread(
  { cell: assumptions.getRange("B22") },
  "Planning placeholder for the incremental PoE-capable switch, UPS and network capacity required by the deployment. Replace with a vendor quote.",
);
workbook.comments.addThread(
  { cell: assumptions.getRange("B27") },
  "Planning energy rate. Replace with the organisation's delivered electricity tariff, including applicable levies.",
);

const category = workbook.worksheets.getItem("Category Model");
for (let r = 5; r <= 20; r += 1) {
  category.getRange(`T${r}:AE${r}`).formulas = [[
    `=IF(COUNT(C${r})=0,"",SUM(P${r}:S${r}))`,
    `=IF(COUNT(C${r})=0,"",ROUND(T${r}/C${r},2))`,
    `=IF(COUNT(C${r})=0,"",C${r}*(F${r}+G${r}+'Assumptions'!$B$18+'Assumptions'!$B$15+'Assumptions'!$B$14*12*'Assumptions'!$B$5))`,
    `=IF(COUNT(C${r})=0,"",ROUND(C${r}*MAX(0,H${r}-'Assumptions'!$B$16)*12*'Assumptions'!$B$5*'Assumptions'!$B$17,2))`,
    `=IF(COUNT(C${r})=0,"",C${r}*F${r}*'Assumptions'!$B$19/100*'Assumptions'!$B$5+C${r}*IF(J${r}<='Assumptions'!$B$5,'Assumptions'!$B$20,0))`,
    `=IF(COUNT(C${r})=0,"",SUM(V${r}:X${r}))`,
    `=IF(COUNT(C${r})=0,"",ROUND(Y${r}/C${r},2))`,
    `=IF(COUNT(C${r})=0,"",ROUND(T${r}-Y${r},2))`,
    `=IF(COUNT(C${r})=0,"Not modelled",IF(OR(K${r}<=2,L${r}<=2),"Not decisive",IF(T${r}<Y${r},"LoRaWAN",IF(T${r}>Y${r},"NB-IoT","Tie"))))`,
    `=IF(COUNT(C${r})=0,"",ABS(T${r}-Y${r})/MAX(T${r},Y${r}))`,
    `=IF(COUNT(C${r})=0,"",IF((Z${r}-U${r}+Q${r}/C${r})<=0,"N/A",ROUND((Q${r}/D${r})/(Z${r}-U${r}+Q${r}/C${r}),0)))`,
    `=IF(COUNT(C${r})=0,"Not modelled",IF(OR(K${r}<=2,L${r}<=2),"Technical constraint dominates cost",IF(AB${r}=N${r},"Cost and architecture align","Review coverage, power and support model")))`,
  ]];
}
category.getRange("T5:AE20").format.font = { color: palette.black };
category.getRange("T5:Z20").format.numberFormat = "£#,##0.00;[Red](£#,##0.00);-";
category.getRange("AA5:AA20").format.numberFormat = "£#,##0.00;[Red](£#,##0.00);-";
category.getRange("AC5:AC20").format.numberFormat = "0.0%";

let poe = workbook.worksheets.items.find((s) => s.name === "PoE Model");
if (!poe) poe = workbook.worksheets.add("PoE Model");
poe.showGridLines = false;
poe.getRange("A1:P20").clear({ applyTo: "all" });
poe.getRange("A1:P1").merge();
poe.getRange("A2:P2").merge();
poe.getRange("A1").values = [["PoE Category-Level Five-Year TCO"]];
poe.getRange("A2").values = [["PoE is a wired power-and-data architecture. Each row links to the corresponding deployment quantity and site count in Category Model; edit the blue PoE inputs below."]];
poe.getRange("A4:P4").values = [[
  "Sensor / use case", "Deployment archetype", "Units", "Sites", "PoE HW £/unit",
  "Cabling & install £/unit", "Average W/device", "PoE suitability (1–5)",
  "Required behaviour / boundary", "PoE technical recommendation", "Confidence",
  "Endpoint capex", "Shared network", "Energy, support & maintenance", "PoE TCO", "PoE TCO/unit",
]];

const poeInputs = [
  [190, 180, 4, 5, "Continuous power and IP data suit fixed indoor IAQ endpoints", "PoE is attractive where structured cabling exists; LPWAN remains simpler for retrofit or dispersed sites", "High"],
  [160, 350, 3, 2, "Outdoor bay devices need protected cabling and civil works", "Use PoE only where ducts and nearby network cabinets already exist", "Low"],
  [240, 160, 12, 5, "Rich displays benefit from continuous power and Ethernet bandwidth", "PoE is preferred for interactive or frequently updated room displays", "High"],
  [85, 150, 2, 5, "Fixed ceiling/wall sensors are compatible with structured cabling", "PoE is strong in new-build or refurbished estates with spare ports", "High"],
  [130, 220, 4, 4, "Plantroom meters may be near Ethernet but isolation and interfaces vary", "PoE is suitable when meters/controllers are fixed and cabling is practical", "Medium"],
  [95, 180, 3, 4, "Fixed critical assets benefit from reliable power and local LAN monitoring", "PoE is suitable when cable routing does not compromise hygiene or operations", "Medium"],
  [350, 600, 6, 2, "Remote wet environments make copper cabling and surge protection costly", "Prefer wireless/cellular unless powered network infrastructure is already present", "Low"],
  [110, 250, 3, 3, "Plantroom locations may be cableable; remote assets often are not", "PoE is conditional on route length, containment and electrical environment", "Medium"],
  [140, 300, 4, 3, "Fixed flow devices may support wired gateways or controllers", "PoE is viable on-site but usually weak for geographically dispersed meters", "Medium"],
  [90, 180, 2, 4, "Fixed leak sensors can use PoE but cabling cost may exceed device cost", "Use PoE for accessible fixed locations; retain local shut-off logic", "Medium"],
  [190, 250, 5, 4, "Feature-rich acoustic monitoring benefits from continuous power and bandwidth", "PoE is strong for machinery/buildings; cellular remains useful for dispersed water networks", "High"],
  [180, 500, 4, 2, "Distributed bins rarely have economical Ethernet cabling", "Use PoE only for fixed indoor waste points with existing cabling", "Low"],
  [550, 350, 15, 5, "Images and edge inference need continuous power and higher bandwidth", "PoE is the preferred option for cameras and optical sensors within cable reach", "High"],
  [150, 220, 6, 5, "High-rate waveform or frequent features benefit from wired bandwidth", "PoE is preferred for fixed machinery monitoring where cable routing is safe", "High"],
  [200, 300, 5, 5, "Clinical plantrooms favour supervised wired infrastructure", "Use PoE for supplementary telemetry while retaining the compliant alarm system", "High"],
  [240, 300, 6, 5, "Fixed clinical cylinder/pipe monitoring benefits from power and deterministic LAN access", "PoE is strong for supplementary telemetry; validate isolation and clinical compliance", "High"],
];

for (let i = 0; i < 16; i += 1) {
  const r = i + 5;
  const sourceRow = i + 5;
  poe.getRange(`A${r}:D${r}`).formulas = [[
    `='Category Model'!A${sourceRow}`,
    `='Category Model'!B${sourceRow}`,
    `='Category Model'!C${sourceRow}`,
    `='Category Model'!D${sourceRow}`,
  ]];
  poe.getRange(`E${r}:K${r}`).values = [[...poeInputs[i]]];
  poe.getRange(`L${r}:P${r}`).formulas = [[
    `=IF(COUNT('Category Model'!C${sourceRow})=0,"",C${r}*(E${r}+F${r}+'Assumptions'!$B$25))`,
    `=IF(COUNT('Category Model'!C${sourceRow})=0,"",D${r}*('Assumptions'!$B$22+'Assumptions'!$B$23))`,
    `=IF(COUNT('Category Model'!C${sourceRow})=0,"",ROUND(C${r}*G${r}/1000*8760*'Assumptions'!$B$26/'Assumptions'!$B$27*'Assumptions'!$B$5+D${r}*'Assumptions'!$B$24*'Assumptions'!$B$5+C${r}*E${r}*'Assumptions'!$B$28/100*'Assumptions'!$B$5,2))`,
    `=IF(COUNT('Category Model'!C${sourceRow})=0,"",ROUND(SUM(L${r}:N${r}),2))`,
    `=IF(COUNT('Category Model'!C${sourceRow})=0,"",ROUND(O${r}/C${r},2))`,
  ]];
}

poe.getRange("A1:P1").format = {
  fill: palette.navy,
  font: { bold: true, fontSize: 18, color: palette.white, typeface: "Carlito" },
  rowHeight: 30,
};
poe.getRange("A2:P2").format = {
  fill: palette.pale,
  font: { italic: true, fontSize: 10, color: palette.navy, typeface: "Carlito" },
  wrapText: true,
  rowHeight: 30,
};
poe.getRange("A4:P4").format = {
  fill: palette.poe,
  font: { bold: true, fontSize: 10, color: palette.white, typeface: "Carlito" },
  wrapText: true,
  borders: { preset: "outside", style: "thin", color: palette.line },
  rowHeight: 48,
};
poe.getRange("A5:P20").format = {
  font: { fontSize: 10, color: palette.black, typeface: "Carlito" },
  wrapText: true,
  borders: { insideHorizontal: { style: "thin", color: "#D9E0E7" } },
};
poe.getRange("A5:D20").format.font = { color: palette.green };
poe.getRange("E5:K20").format = {
  fill: palette.input,
  font: { color: palette.blue },
};
poe.getRange("L5:P20").format.font = { color: palette.black };
poe.getRange("E5:F20").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
poe.getRange("G5:G20").setNumberFormat("0.0");
poe.getRange("H5:H20").setNumberFormat("0");
poe.getRange("L5:P20").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
poe.getRange("A5:A20").format.font = { bold: true, color: palette.green };
poe.getRange("A5:P20").format.rowHeight = 42;
for (const [col, width] of Object.entries({
  A: 30, B: 24, C: 10, D: 9, E: 14, F: 18, G: 14, H: 16,
  I: 34, J: 38, K: 12, L: 16, M: 16, N: 23, O: 16, P: 16,
})) poe.getRange(`${col}1:${col}20`).format.columnWidth = width;
poe.freezePanes.freezeRows(4);
poe.freezePanes.freezeColumns(2);

const summary = workbook.worksheets.getItem("Summary");
summary.deleteAllDrawings();
for (const address of ["A1:H1", "A2:H2", "A4:B4", "D4:H4", "D5:H9"]) summary.getRange(address).unmerge();
summary.getRange("A1:Z30").clear({ applyTo: "all" });
summary.showGridLines = false;
summary.getRange("A1:J1").merge();
summary.getRange("A2:J2").merge();
summary.getRange("A1").values = [["LoRaWAN vs NB-IoT vs PoE — Cost & Fit Summary"]];
summary.getRange("A2").values = [["UK planning benchmark • GBP ex VAT • 5-year default • category scenarios are independently editable"]];
summary.getRange("A4:B4").merge();
summary.getRange("D4:J4").merge();
summary.getRange("D5:J10").merge();
summary.getRange("A4").values = [["Model headline"]];
summary.getRange("D4").values = [["What the model says"]];
summary.getRange("A5:A10").values = [
  ["Categories modelled"], ["Configured scenarios"], ["LoRaWAN lower-cost"],
  ["NB-IoT lower-cost"], ["PoE lower-cost"], ["Default horizon"],
];
summary.getRange("B5:B10").formulas = [
  ["=COUNTA('Category Model'!A5:A20)"],
  ["=COUNT('Category Model'!C5:C20)"],
  ["=COUNTIF(H13:H28,\"LoRaWAN\")"],
  ["=COUNTIF(H13:H28,\"NB-IoT\")"],
  ["=COUNTIF(H13:H28,\"PoE\")"],
  ["='Assumptions'!B5"],
];
summary.getRange("D5").values = [[
  "PoE is now modelled as a third architecture, including endpoint hardware, cabling and installation, shared switch/network infrastructure, electricity, support and maintenance. Cost winners exclude options with a suitability score below 3. Blank device quantities remain deliberately unconfigured; enter units and sites in Category Model, then replace blue PoE assumptions with surveyed cable routes, spare-port capacity and vendor quotes.",
]];
summary.getRange("A12:J12").values = [[
  "Sensor / use case", "LoRa fit", "NB fit", "PoE fit", "LoRa TCO/unit",
  "NB TCO/unit", "PoE TCO/unit", "Lowest-cost viable", "Technical recommendation", "Confidence",
]];
for (let i = 0; i < 16; i += 1) {
  const sr = 13 + i;
  const cr = 5 + i;
  summary.getRange(`A${sr}:G${sr}`).formulas = [[
    `='Category Model'!A${cr}`,
    `='Category Model'!K${cr}`,
    `='Category Model'!L${cr}`,
    `='PoE Model'!H${cr}`,
    `='Category Model'!U${cr}`,
    `='Category Model'!Z${cr}`,
    `='PoE Model'!P${cr}`,
  ]];
  summary.getRange(`H${sr}`).formulas = [[
    `=IF(COUNT('Category Model'!C${cr})=0,"Not modelled",IF(MAX(B${sr}:D${sr})<=2,"Technical constraint",IF(IF(B${sr}>=3,E${sr},1E+99)=MIN(IF(B${sr}>=3,E${sr},1E+99),IF(C${sr}>=3,F${sr},1E+99),IF(D${sr}>=3,G${sr},1E+99)),"LoRaWAN",IF(IF(C${sr}>=3,F${sr},1E+99)=MIN(IF(B${sr}>=3,E${sr},1E+99),IF(C${sr}>=3,F${sr},1E+99),IF(D${sr}>=3,G${sr},1E+99)),"NB-IoT","PoE"))))`,
  ]];
  summary.getRange(`I${sr}`).formulas = [[
    `=IF(H${sr}="PoE",'PoE Model'!J${cr},IF(H${sr}="Not modelled","Enter device quantity in Category Model",'Category Model'!N${cr}))`,
  ]];
  summary.getRange(`J${sr}`).formulas = [[
    `=IF(H${sr}="PoE",'PoE Model'!K${cr},'Category Model'!O${cr})`,
  ]];
  summary.getRange(`W${sr}:Z${sr}`).formulas = [[`=A${sr}`, `=E${sr}`, `=F${sr}`, `=G${sr}`]];
}
summary.getRange("W12:Z12").values = [["Sensor / use case", "LoRaWAN", "NB-IoT", "PoE"]];
summary.getRange("A1:J1").format = {
  fill: palette.navy,
  font: { bold: true, fontSize: 18, color: palette.white, typeface: "Carlito" },
  rowHeight: 30,
};
summary.getRange("A2:J2").format = {
  fill: palette.pale,
  font: { italic: true, fontSize: 10, color: palette.navy, typeface: "Carlito" },
  rowHeight: 28,
};
summary.getRange("A4:B4").format = summary.getRange("D4:J4").format = {
  fill: palette.navy,
  font: { bold: true, fontSize: 11, color: palette.white, typeface: "Carlito" },
};
summary.getRange("A5:B10").format = {
  fill: "#F4F6F8",
  font: { fontSize: 11, color: palette.navy, typeface: "Carlito" },
  borders: { bottom: { style: "thin", color: palette.line } },
};
summary.getRange("A5:A10").format.font = { bold: true, color: "#657087" };
summary.getRange("B5:B10").format.font = { bold: true, fontSize: 13, color: palette.green };
summary.getRange("B10").setNumberFormat('0 "years"');
summary.getRange("D5:J10").format = {
  fill: palette.note,
  font: { bold: true, fontSize: 10, color: palette.noteText, typeface: "Carlito" },
  wrapText: true,
  verticalAlignment: "center",
  borders: { preset: "outside", style: "thin", color: "#D6B656" },
};
summary.getRange("A12:J12").format = {
  fill: palette.teal,
  font: { bold: true, fontSize: 10, color: palette.white, typeface: "Carlito" },
  wrapText: true,
  rowHeight: 42,
};
summary.getRange("A13:J28").format = {
  font: { fontSize: 10, color: palette.black, typeface: "Carlito" },
  wrapText: true,
  borders: { insideHorizontal: { style: "thin", color: "#D9E0E7" } },
  rowHeight: 34,
};
summary.getRange("A13:J28").format.font = { color: palette.green };
summary.getRange("H13:H28").format.font = { color: palette.black };
summary.getRange("A13:A28").format.font = { bold: true, color: palette.green };
summary.getRange("E13:G28").setNumberFormat("£#,##0.00;[Red](£#,##0.00);-");
summary.getRange("I13:I28").format.fill = "#D9F0ED";
summary.getRange("H13:H28").conditionalFormats.add("containsText", { text: "PoE", format: { fill: palette.softPoe, font: { bold: true, color: palette.poe } } });
summary.getRange("H13:H28").conditionalFormats.add("containsText", { text: "LoRaWAN", format: { fill: "#DDEBF7", font: { bold: true, color: "#1F4E78" } } });
summary.getRange("H13:H28").conditionalFormats.add("containsText", { text: "NB-IoT", format: { fill: "#FCE4D6", font: { bold: true, color: "#C65911" } } });
for (const [col, width] of Object.entries({ A: 28, B: 10, C: 10, D: 10, E: 15, F: 15, G: 15, H: 18, I: 48, J: 12 })) {
  summary.getRange(`${col}1:${col}28`).format.columnWidth = width;
}
summary.freezePanes.freezeRows(12);

const chart = summary.charts.add("bar", summary.getRange("W12:Z28"));
chart.title = "Five-year TCO per device by category (£)";
chart.hasLegend = true;
chart.yAxis = { numberFormatCode: "£#,##0" };
chart.setPosition("L4", "U28");

const guide = workbook.worksheets.getItem("Decision Guide");
for (const address of ["A1:F1", "A2:F2", "A15:F15"]) guide.getRange(address).unmerge();
guide.getRange("A1:G15").clear({ applyTo: "all" });
guide.getRange("A1:G1").merge();
guide.getRange("A2:G2").merge();
guide.getRange("A15:G15").merge();
guide.getRange("A1").values = [["Decision Guide — LoRaWAN, NB-IoT and PoE"]];
guide.getRange("A2").values = [["Cost follows architecture. Apply these gates before accepting the cheapest five-year TCO."]];
guide.getRange("A4:G4").values = [["Question", "LoRaWAN signal", "NB-IoT signal", "PoE signal", "Preferred direction", "Risk / caveat", "Action"]];
guide.getRange("A5:G14").values = [
  ["Are devices concentrated in owned buildings/campus?", "Strong", "Neutral", "Strong if cableable", "LoRaWAN or PoE", "PoE cable routes and switch capacity may dominate cost", "RF survey plus structured-cabling and port-capacity survey"],
  ["Are devices geographically dispersed?", "Weak unless public LoRaWAN exists", "Strong", "Weak", "NB-IoT", "Carrier coverage varies; PoE usually needs costly civil works", "Test target SIM/device and map any existing powered network"],
  ["Is structured cabling already available near endpoints?", "Neutral", "Neutral", "Strong", "PoE", "Spare cable does not guarantee spare switch power, UPS or ports", "Audit patch panels, port budgets, UPS runtime and VLAN capacity"],
  ["Is payload small and infrequent?", "Strong", "Strong", "Capable but potentially overbuilt", "LoRaWAN or NB-IoT", "Architecture overhead can outweigh sensor cost", "Define bytes/message, messages/day and alarm bursts"],
  ["Are images, video, raw audio or raw waveforms required?", "Unsuitable", "Usually unsuitable", "Strong", "PoE / wired Ethernet", "Cable reach and power class must match the endpoint", "Specify bandwidth, power class, storage and cyber controls"],
  ["Is an immediate local safety action required?", "Cloud path insufficient", "Cloud path insufficient", "Network path still insufficient alone", "Local/wired fail-safe", "No remote telemetry path should be the sole safety layer", "Keep compliant local control and use telemetry for supervision"],
  ["Will a battery visit be expensive?", "Often strongest", "Good", "No battery at endpoint", "PoE if cableable; otherwise compare LPWAN", "PoE avoids battery visits but adds cable and switch lifecycle costs", "Compare surveyed cable cost with lifetime battery service cost"],
  ["Do you require frequent two-way control?", "Limited downlink capacity", "Better but still LPWA", "Strong", "PoE", "Availability now depends on LAN, switch and UPS design", "Define latency, redundancy, UPS and recovery requirements"],
  ["Do you need to avoid local infrastructure?", "No", "Yes", "No", "NB-IoT", "Recurring carrier dependency replaces gateway/switch ownership", "Review contract, roaming, coverage and exit strategy"],
  ["Is the endpoint more than 100 m of copper path from a switch?", "N/A", "N/A", "Major constraint", "Wireless, fibre plus local power, or redesign", "Standard twisted-pair Ethernet reach is 100 m", "Survey permanent link, patching, containment and surge exposure"],
];
guide.getRange("A15").values = [["Suitability score: 5 = natural fit; 4 = good fit; 3 = conditional; 2 = major constraint; 1 = unsuitable for the stated behaviour."]];
guide.getRange("A1:G1").format = {
  fill: palette.navy,
  font: { bold: true, fontSize: 18, color: palette.white, typeface: "Carlito" },
};
guide.getRange("A2:G2").format = {
  fill: palette.pale,
  font: { italic: true, fontSize: 10, color: palette.navy, typeface: "Carlito" },
  wrapText: true,
};
guide.getRange("A4:G4").format = {
  fill: palette.teal,
  font: { bold: true, fontSize: 10, color: palette.white, typeface: "Carlito" },
  wrapText: true,
  rowHeight: 38,
};
guide.getRange("A5:G14").format = {
  font: { fontSize: 10, color: palette.black, typeface: "Carlito" },
  wrapText: true,
  borders: { insideHorizontal: { style: "thin", color: "#D9E0E7" } },
  rowHeight: 45,
};
guide.getRange("D5:D14").format.fill = palette.softPoe;
guide.getRange("A15:G15").format = {
  fill: palette.note,
  font: { italic: true, fontSize: 10, color: palette.noteText, typeface: "Carlito" },
  wrapText: true,
  rowHeight: 34,
};
for (const [col, width] of Object.entries({ A: 34, B: 22, C: 22, D: 24, E: 25, F: 38, G: 38 })) {
  guide.getRange(`${col}1:${col}15`).format.columnWidth = width;
}
guide.freezePanes.freezeRows(4);

const sources = workbook.worksheets.getItem("Sources");
sources.getRange("A19:F20").values = [
  ["PoE standards and power classes", "IEEE 802.3bt uses all four pairs and extends standardized power delivery", "https://standards.ieee.org/ieee/802.3bt/6749/", new Date(2026, 6, 31), "Standards body", "PoE technical boundary and power-delivery context"],
  ["PoE copper reach", "Standard maximum twisted-pair Ethernet cable length is 100 m", "https://www.cisco.com/c/en/us/td/docs/switches/lan/catalyst3750/software/troubleshooting/g_power_over_ethernet.html", new Date(2026, 6, 31), "Vendor technical guide", "Decision gate for cable-route feasibility"],
];
sources.getRange("A19:F20").format = {
  font: { fontSize: 10, color: palette.black, typeface: "Carlito" },
  wrapText: true,
  borders: { insideHorizontal: { style: "thin", color: "#D9E0E7" } },
  rowHeight: 48,
};
sources.getRange("D19:D20").format.numberFormat = "dd mmm yyyy";

let checks = workbook.worksheets.items.find((s) => s.name === "Checks");
if (!checks) checks = workbook.worksheets.add("Checks");
checks.showGridLines = false;
checks.getRange("A1:G10").clear({ applyTo: "all" });
checks.getRange("A1:G1").merge();
checks.getRange("A2:G2").merge();
checks.getRange("A1").values = [["Model Checks"]];
checks.getRange("A2").values = [["One assertion per row. The model status is OK only when all configured calculations tie and all editable cost assumptions are non-negative."]];
checks.getRange("A4:G4").values = [["Check", "Actual", "Expected", "Difference", "Tolerance", "Status", "Notes"]];
checks.getRange("A5:A8").values = [
  ["PoE TCO components tie"],
  ["Configured PoE rows have unit TCO"],
  ["PoE inputs are non-negative"],
  ["Summary category count ties"],
];
checks.getRange("B5:E8").formulas = [
  ["=SUM('PoE Model'!O5:O20)-SUM('PoE Model'!L5:N20)", "=0", "=B5-C5", "=0.01"],
  ["=COUNT('PoE Model'!P5:P20)", "=COUNT('Category Model'!C5:C20)", "=B6-C6", "=0"],
  ["=MIN('Assumptions'!B22:B28,'PoE Model'!E5:H20)", "=0", "=MIN(0,B7-C7)", "=0"],
  ["='Summary'!B5", "=COUNTA('Category Model'!A5:A20)", "=B8-C8", "=0"],
];
checks.getRange("F5:F8").formulas = [
  ['=IF(ABS(D5)<=E5,"OK","FAIL")'],
  ['=IF(ABS(D6)<=E6,"OK","FAIL")'],
  ['=IF(D7>=-E7,"OK","FAIL")'],
  ['=IF(ABS(D8)<=E8,"OK","FAIL")'],
];
checks.getRange("G5:G8").values = [
  ["Total PoE TCO must equal endpoint capex + shared network + energy/support/maintenance."],
  ["Every row with a device quantity must produce a PoE TCO per unit."],
  ["Negative cost, power, suitability or efficiency inputs are not permitted."],
  ["Summary must cover all Category Model use cases."],
];
checks.getRange("A10:E10").merge();
checks.getRange("A10").values = [["Overall model status"]];
checks.getRange("F10:G10").merge();
checks.getRange("F10").formulas = [['=IF(COUNTIF(F5:F8,"FAIL")=0,"OK","FAIL")']];
checks.getRange("A1:G1").format = {
  fill: palette.navy,
  font: { bold: true, fontSize: 18, color: palette.white, typeface: "Carlito" },
};
checks.getRange("A2:G2").format = {
  fill: palette.pale,
  font: { italic: true, fontSize: 10, color: palette.navy, typeface: "Carlito" },
  wrapText: true,
};
checks.getRange("A4:G4").format = {
  fill: palette.teal,
  font: { bold: true, fontSize: 10, color: palette.white, typeface: "Carlito" },
};
checks.getRange("A5:G8").format = {
  font: { fontSize: 10, color: palette.black, typeface: "Carlito" },
  wrapText: true,
  borders: { insideHorizontal: { style: "thin", color: "#D9E0E7" } },
  rowHeight: 34,
};
checks.getRange("B5:E8").setNumberFormat("#,##0.00;[Red](#,##0.00);-");
checks.getRange("A10:G10").format = {
  fill: palette.navy,
  font: { bold: true, fontSize: 12, color: palette.white, typeface: "Carlito" },
};
checks.getRange("F5:F8").conditionalFormats.add("containsText", { text: "OK", format: { fill: palette.ok, font: { bold: true, color: palette.okText } } });
checks.getRange("F5:F8").conditionalFormats.add("containsText", { text: "FAIL", format: { fill: palette.fail, font: { bold: true, color: palette.failText } } });
for (const [col, width] of Object.entries({ A: 32, B: 14, C: 14, D: 14, E: 12, F: 12, G: 60 })) {
  checks.getRange(`${col}1:${col}10`).format.columnWidth = width;
}
checks.freezePanes.freezeRows(4);

for (let i = 0; i < workbook.worksheets.items.length; i += 1) {
  const sheet = workbook.worksheets.getItemAt(i);
  const preview = await workbook.render({ sheetName: sheet.name, autoCrop: "all", scale: 1.25, format: "png" });
  await fs.writeFile(`${base}/after_${i}_${sheet.name.replaceAll(/[^A-Za-z0-9_-]/g, "_")}.png`, new Uint8Array(await preview.arrayBuffer()));
}

const keySummary = await workbook.inspect({
  kind: "table",
  range: "Summary!A4:J28",
  include: "values,formulas",
  tableMaxRows: 30,
  tableMaxCols: 12,
  maxChars: 14000,
});
console.log("KEY SUMMARY");
console.log(keySummary.ndjson);

const poeCheck = await workbook.inspect({
  kind: "table",
  range: "'PoE Model'!A4:P20",
  include: "values,formulas",
  tableMaxRows: 20,
  tableMaxCols: 18,
  maxChars: 14000,
});
console.log("POE MODEL");
console.log(poeCheck.ndjson);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 300 },
  summary: "final formula error scan",
});
console.log("ERROR SCAN");
console.log(errors.ndjson);

const formatCheck = await workbook.inspect({
  kind: "computedStyle",
  sheetId: "PoE Model",
  range: "E5:P5",
  maxChars: 5000,
});
console.log("FORMAT CHECK");
console.log(formatCheck.ndjson);

await fs.mkdir(base, { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);
console.log(`OUTPUT ${outputPath}`);
