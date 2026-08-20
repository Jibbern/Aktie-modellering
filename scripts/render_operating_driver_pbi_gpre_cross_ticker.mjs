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
  ?? "C:/Users/Jibbe/Aktier/StockModelData/audit/operating_drivers_pbi_gpre_cross_ticker_2026-08-20";
const build = JSON.parse(await fs.readFile(path.join(auditRoot, "work", "BUILD_RESULTS.json"), "utf8"));
const renderRoot = path.join(auditRoot, "work", "renders");
await fs.mkdir(renderRoot, { recursive: true });

function sha256(bytes) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}

function rowFromRange(value) {
  return Number(value.match(/\d+$/)[0]);
}

const receipt = {
  authoring_used: false,
  render_contract: "artifact-tool read-inspection-render-only@1",
  tickers: {},
};

for (const ticker of ["PBI", "GPRE"]) {
  const plan = JSON.parse(await fs.readFile(path.join(auditRoot, "work", `${ticker}_WORKBOOK_PLAN.json`), "utf8"));
  const candidate = await SpreadsheetFile.importXlsx(await FileBlob.load(build.outputs[ticker]));
  const replay = await SpreadsheetFile.importXlsx(await FileBlob.load(build.replays[ticker]));
  const coreRow = plan.major_section_rows["Core Drivers"];
  const historyRow = plan.major_section_rows["Quarterly Driver History"];
  const finalRow = rowFromRange(plan.used_range);
  const guideDataRows = Object.values(plan.guide_rows);
  const guideSectionRow = guideDataRows.length ? Math.min(...guideDataRows) - 2 : finalRow + 1;
  const historyLastRow = guideDataRows.length ? guideSectionRow - 2 : finalRow;
  const views = {
    full_sheet: { range: plan.used_range, scale: 0.9 },
    overview: { range: `A1:P${coreRow - 1}`, scale: 1.0 },
    core_drivers: { range: `A${coreRow}:P${historyRow - 2}`, scale: 1.0 },
    quarterly_history: { range: `A${historyRow}:P${historyLastRow}`, scale: 1.0 },
    driver_guide: { range: `A${guideSectionRow}:P${finalRow}`, scale: 1.1 },
  };
  const tickerRoot = path.join(renderRoot, ticker.toLowerCase());
  await fs.mkdir(tickerRoot, { recursive: true });
  const rendered = {};
  for (const [name, spec] of Object.entries(views)) {
    const blobA = await candidate.render({ sheetName: "Operating_Drivers", range: spec.range, scale: spec.scale, format: "png" });
    const blobB = await replay.render({ sheetName: "Operating_Drivers", range: spec.range, scale: spec.scale, format: "png" });
    const bytesA = new Uint8Array(await blobA.arrayBuffer());
    const bytesB = new Uint8Array(await blobB.arrayBuffer());
    const hashA = sha256(bytesA);
    const hashB = sha256(bytesB);
    if (hashA !== hashB) {
      throw new Error(`${ticker} ${name} render is nondeterministic: ${hashA} != ${hashB}`);
    }
    const output = path.join(tickerRoot, `${name}.png`);
    await fs.writeFile(output, bytesA);
    rendered[name] = {
      path: output.replaceAll("\\", "/"),
      range: spec.range,
      scale: spec.scale,
      sha256: hashA,
      replay_sha256: hashB,
      replay_match: true,
      size: bytesA.byteLength,
    };
  }
  receipt.tickers[ticker] = { views: rendered };
}

await fs.writeFile(
  path.join(auditRoot, "work", "RENDER_RESULTS.json"),
  `${JSON.stringify(receipt, null, 2)}\n`,
  "utf8",
);
console.log(JSON.stringify(receipt, null, 2));
