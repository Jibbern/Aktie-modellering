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
  ?? "C:/Users/Jibbe/Aktier/StockModelData/audit/anf_operating_drivers_final_information_density_2026-08-20";
const build = JSON.parse(
  await fs.readFile(path.join(auditRoot, "work", "BUILD_RESULTS.json"), "utf8"),
);
const candidateA = await SpreadsheetFile.importXlsx(await FileBlob.load(build.candidate_a));
const candidateB = await SpreadsheetFile.importXlsx(await FileBlob.load(build.candidate_b));
const renderRoot = path.join(auditRoot, "work", "renders");
await fs.mkdir(renderRoot, { recursive: true });

const views = {
  full_sheet: { range: "A1:P52", scale: 1.0 },
  operating_interpretation_overview: { range: "A1:P17", scale: 1.15 },
  core_drivers: { range: "A18:P31", scale: 1.1 },
  quarterly_driver_history: { range: "A32:P52", scale: 1.0 },
};

function sha256(bytes) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}

const receipt = {
  authoring_used: false,
  render_contract: "artifact-tool read-inspection-render-only@1",
  views: {},
};
for (const [name, spec] of Object.entries(views)) {
  const blobA = await candidateA.render({
    sheetName: "Operating_Drivers",
    range: spec.range,
    scale: spec.scale,
    format: "png",
  });
  const blobB = await candidateB.render({
    sheetName: "Operating_Drivers",
    range: spec.range,
    scale: spec.scale,
    format: "png",
  });
  const bytesA = new Uint8Array(await blobA.arrayBuffer());
  const bytesB = new Uint8Array(await blobB.arrayBuffer());
  const hashA = sha256(bytesA);
  const hashB = sha256(bytesB);
  if (hashA !== hashB) {
    throw new Error(`${name} render is nondeterministic: ${hashA} != ${hashB}`);
  }
  const output = path.join(renderRoot, `${name}.png`);
  await fs.writeFile(output, bytesA);
  receipt.views[name] = {
    path: output.replaceAll("\\", "/"),
    range: spec.range,
    scale: spec.scale,
    sha256: hashA,
    replay_sha256: hashB,
    replay_match: true,
    size: bytesA.byteLength,
  };
}
await fs.writeFile(
  path.join(auditRoot, "work", "RENDER_RESULTS.json"),
  `${JSON.stringify(receipt, null, 2)}\n`,
  "utf8",
);
console.log(JSON.stringify(receipt, null, 2));
