import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const dependencyRoot = process.env.CODEX_PRIMARY_NODE_MODULES;
const artifactTool = dependencyRoot
  ? await import(pathToFileURL(path.join(dependencyRoot, "@oai", "artifact-tool", "dist", "artifact_tool.mjs")).href)
  : await import("@oai/artifact-tool");
const { FileBlob, SpreadsheetFile } = artifactTool;

const auditRoot = process.argv[2]
  ?? "C:/Users/Jibbe/Aktier/StockModelData/audit/operating_drivers_golden_acceptance_2026-08-20";
const goldenRoot = path.join(auditRoot, "golden");
const replayRoot = path.join(auditRoot, "work", "registered_replay_current");
const renderRoot = path.join(auditRoot, "work", "renders");
await fs.mkdir(renderRoot, { recursive: true });

const specs = {
  ANF: {
    file: "ANF_operating_drivers_source_native_golden_v1.xlsx",
    expectedFull: "7a38ffdef8e6e6ce998b975d6f1c95d1337860d832ff14a8ff144ebce8bc39e0",
    views: {
      full_sheet: { range: "A1:P61", scale: 1.0 },
      operating_interpretation: { range: "A1:P17", scale: 1.0 },
      core_drivers: { range: "A18:P30", scale: 1.0 },
      quarterly_history: { range: "A32:P52", scale: 1.0 },
      driver_guide: { range: "A54:P61", scale: 1.15 },
    },
  },
  PBI: {
    file: "PBI_operating_drivers_source_native_golden_v1.xlsx",
    expectedFull: "7d0497da5f8e8db9dc72bd955424236e551262542664a795ec3e04b72319af03",
    views: {
      full_sheet: { range: "A1:P45", scale: 0.9 },
      operating_interpretation: { range: "A1:P18", scale: 1.0 },
      core_drivers: { range: "A19:P27", scale: 1.0 },
      quarterly_history: { range: "A29:P37", scale: 1.0 },
      driver_guide: { range: "A39:P45", scale: 1.1 },
    },
  },
  GPRE: {
    file: "GPRE_operating_drivers_source_native_golden_v1.xlsm",
    expectedFull: "c84db06f1363a779af96e949fe676ba80679fcb84182e9ea382ffc9d48a2d69f",
    views: {
      full_sheet: { range: "A1:P55", scale: 0.9 },
      operating_interpretation: { range: "A1:P18", scale: 1.0 },
      core_drivers: { range: "A19:P28", scale: 1.0 },
      quarterly_history: { range: "A30:P46", scale: 1.0 },
      driver_guide: { range: "A48:P55", scale: 1.1 },
    },
  },
};

function sha256(bytes) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}

const receipt = {
  authoring_used: false,
  render_contract: "artifact-tool-operating-drivers-golden-read-inspection-render-only@1",
  result: "PASS",
  tickers: {},
};

for (const [ticker, spec] of Object.entries(specs)) {
  const accepted = await SpreadsheetFile.importXlsx(await FileBlob.load(path.join(goldenRoot, spec.file)));
  const replay = await SpreadsheetFile.importXlsx(await FileBlob.load(path.join(replayRoot, spec.file)));
  const tickerRoot = path.join(renderRoot, ticker.toLowerCase());
  await fs.mkdir(tickerRoot, { recursive: true });
  const views = {};
  for (const [name, view] of Object.entries(spec.views)) {
    const blobA = await accepted.render({ sheetName: "Operating_Drivers", range: view.range, scale: view.scale, format: "png" });
    const blobB = await replay.render({ sheetName: "Operating_Drivers", range: view.range, scale: view.scale, format: "png" });
    const bytesA = new Uint8Array(await blobA.arrayBuffer());
    const bytesB = new Uint8Array(await blobB.arrayBuffer());
    const hashA = sha256(bytesA);
    const hashB = sha256(bytesB);
    if (hashA !== hashB) {
      throw new Error(`${ticker} ${name} render replay changed: ${hashA} != ${hashB}`);
    }
    if (name === "full_sheet" && hashA !== spec.expectedFull) {
      throw new Error(`${ticker} accepted full-sheet render changed: ${hashA} != ${spec.expectedFull}`);
    }
    const output = path.join(tickerRoot, `${name}.png`);
    await fs.writeFile(output, bytesA);
    views[name] = {
      path: output.replaceAll("\\", "/"),
      range: view.range,
      scale: view.scale,
      sha256: hashA,
      replay_sha256: hashB,
      replay_match: true,
      size: bytesA.byteLength,
    };
  }
  receipt.tickers[ticker] = { views };
}

await fs.writeFile(
  path.join(auditRoot, "work", "RENDER_RESULTS.json"),
  `${JSON.stringify(receipt, null, 2)}\n`,
  "utf8",
);
console.log(JSON.stringify(receipt, null, 2));
