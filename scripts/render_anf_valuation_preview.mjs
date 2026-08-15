/** Read-only artifact-tool inspection and render for a Valuation preview. */
import fs from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const artifactToolModule = process.env.CODEX_ARTIFACT_TOOL_MODULE || "@oai/artifact-tool";
const artifactTool = await import(
  path.isAbsolute(artifactToolModule)
    ? pathToFileURL(artifactToolModule).href
    : artifactToolModule
);
const { FileBlob, SpreadsheetFile } = artifactTool;

const [workbookPath, outputDir] = process.argv.slice(2);
if (!workbookPath || !outputDir) {
  throw new Error("Usage: render_anf_valuation_preview.mjs <workbook.xlsx> <output-dir>");
}

await fs.mkdir(outputDir, { recursive: true });
const input = await FileBlob.load(path.resolve(workbookPath));
const workbook = await SpreadsheetFile.importXlsx(input);
const inspect = await workbook.inspect({
  kind: "table",
  range: "Valuation!A1:AI261",
  include: "values,formulas",
  tableMaxRows: 261,
  tableMaxCols: 35,
});
await fs.writeFile(
  path.join(outputDir, "valuation_artifact_inspect.json"),
  `${JSON.stringify(inspect, null, 2)}\n`,
  "utf8",
);
const preview = await workbook.render({
  sheetName: "Valuation",
  autoCrop: "all",
  scale: 1,
  format: "png",
});
await fs.writeFile(
  path.join(outputDir, "valuation_complete.png"),
  new Uint8Array(await preview.arrayBuffer()),
);
console.log(JSON.stringify({
  artifactToolRole: "READ / INSPECTION / RENDER ONLY",
  outputDir: path.resolve(outputDir),
  status: "ok",
}));
