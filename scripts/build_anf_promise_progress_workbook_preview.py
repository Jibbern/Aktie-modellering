from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from pbi_xbrl.longitudinal_memory.promise_progress_projection import (
    build_promise_progress_product,
    serialize_promise_progress_product,
    serialize_shadow_matrix,
)
from pbi_xbrl.longitudinal_memory.sector_packs.retail import RETAIL_SECTOR_PACK
from pbi_xbrl.longitudinal_memory.source_adapter.builder import build_source_native_sidecar
from pbi_xbrl.longitudinal_memory.ticker_profiles.anf import load_anf_profile
from pbi_xbrl.promise_progress_workbook_preview import (
    EXPECTED_ANF_PRODUCT_SHA256,
    EXPECTED_ANF_SHADOW_SHA256,
    EXPECTED_ANF_WORKBOOK_SHA256,
    PromiseProgressWorkbookPreviewError,
    build_legacy_difference_report,
    build_preview_manifest,
    build_promise_progress_workbook_binding_plan,
    build_workbook_trace,
    canonical_json_bytes,
    load_json_strict,
    materialize_promise_progress_preview,
    sha256_file,
    validate_preview_semantics,
    validate_preview_structure,
    validate_preview_visual_fit,
    write_deterministic_json,
)


DEFAULT_SOURCE_ROOT = Path(r"C:\Users\Jibbe\Aktier\StockModelData")
DEFAULT_LEGACY_WORKBOOK = (
    DEFAULT_SOURCE_ROOT / "outputs" / "Excel stock models" / "ANF_model.xlsx"
)
DEFAULT_DESIGN_LOCK_ROOT = DEFAULT_SOURCE_ROOT / "audit" / "promise_progress_design_lock"
DEFAULT_OUTPUT_ROOT = DEFAULT_SOURCE_ROOT / "audit" / "promise_progress_workbook_preview"
PREVIEW_NAME = "ANF_Promise_Progress_source_native_preview.xlsx"
REPEAT_NAME = "ANF_Promise_Progress_source_native_preview.repeat.xlsx"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _build_product(source_root: Path):
    oracle_path = REPOSITORY_ROOT / "tests" / "fixtures" / "promise_progress" / "anf_legacy_oracle.v1.json"
    oracle = load_json_strict(oracle_path)
    package = build_source_native_sidecar(
        REPOSITORY_ROOT / oracle["source_package_fixture"],
        source_root=source_root,
        reviewed_model_root=REPOSITORY_ROOT,
        sector_pack=RETAIL_SECTOR_PACK,
        ticker_profile_loader=load_anf_profile,
    ).package
    product = build_promise_progress_product(package, oracle["projection_plan"])
    product_sha = _sha256(serialize_promise_progress_product(product))
    shadow_sha = _sha256(serialize_shadow_matrix(product))
    if product_sha != EXPECTED_ANF_PRODUCT_SHA256 or shadow_sha != EXPECTED_ANF_SHADOW_SHA256:
        raise PromiseProgressWorkbookPreviewError(
            f"accepted ANF Promise Progress product changed: product={product_sha}, shadow={shadow_sha}"
        )
    return product


def _visual_markdown(
    *,
    preview_path: Path,
    structural: dict,
    semantic: dict,
    visual_fit: dict,
    render_images: tuple[Path, ...],
    visual_result: str,
    visual_notes: str,
) -> str:
    status_counts: dict[str, int] = {}
    for row in structural["status_style_results"]:
        status_counts[row["status_code"]] = status_counts.get(row["status_code"], 0) + 1
    lines = [
        "# Promise Progress workbook preview visual validation",
        "",
        f"- Preview: `{preview_path}`",
        "- Sheet: `Promise_Progress_UI`",
        "- Scope: reviewed PresentationContract@2 over `A1:L102`; `M:O` remain hidden support columns.",
        "- Method: structural OOXML validation plus deterministic local workbook rendering; Microsoft Excel was not used.",
        f"- Structural validation: **{'PASS' if structural['passed'] else 'FAIL'}**",
        f"- Semantic validation: **{'PASS' if semantic['passed'] else 'FAIL'}**",
        f"- Deterministic fit validation: **{'PASS' if visual_fit['passed'] else 'FAIL'}**",
        f"- Clipped visible fields: **{visual_fit['clipped_visible_field_count']}**",
        f"- Overflow dependencies: **{visual_fit['overflow_dependency_count']}**",
        f"- Visual inspection: **{visual_result.upper()}**",
        f"- Status styles: `{json.dumps(status_counts, sort_keys=True)}`",
        "",
        "## Reviewed layout checks",
        "",
        "Sheet identity, block order, A1:O102, A2 freeze, 112% zoom, hidden-column semantics, palette, typography and investor workflow remain exact. A:L now uses the reviewed U24 grid, role-based spans, explicit wrapping and deterministic 24/40/56/72 point data-row tiers.",
        "",
        "## Source-native checks",
        "",
        "Actual, Progress, Status, period, source/note, missing-state and row identity are written from the immutable product binding. Missing values remain blank; legacy values are never used as fallback.",
        "",
        "## Inspection notes",
        "",
        visual_notes,
    ]
    if render_images:
        lines.extend(
            [
                "",
                "## Render artifacts",
                "",
            ]
        )
        lines.extend(f"- `{path.name}` — `{sha256_file(path)}`" for path in render_images)
    return "\n".join(lines).rstrip() + "\n"


def _existing_artifact_paths(output_root: Path, render_images: tuple[Path, ...]) -> list[Path]:
    paths = [
        output_root / PREVIEW_NAME,
        output_root / REPEAT_NAME,
        output_root / "binding_plan.json",
        output_root / "presentation_contract_v2.json",
        output_root / "workbook_trace.json",
        output_root / "structural_validation.json",
        output_root / "semantic_validation.json",
        output_root / "visual_fit_validation.json",
        output_root / "legacy_difference_report.json",
        output_root / "visual_validation.md",
    ]
    metrics = output_root / "visual_metrics.json"
    if metrics.is_file():
        paths.append(metrics)
    paths.extend(render_images)
    return paths


def _refresh_visual_and_manifest(
    *,
    product,
    plan,
    legacy_workbook: Path,
    design_lock_root: Path,
    output_root: Path,
    render_images: tuple[Path, ...],
    visual_result: str,
    visual_notes: str,
) -> dict:
    preview_path = output_root / PREVIEW_NAME
    repeat_path = output_root / REPEAT_NAME
    structural = load_json_strict(output_root / "structural_validation.json")
    semantic = load_json_strict(output_root / "semantic_validation.json")
    visual_fit = load_json_strict(output_root / "visual_fit_validation.json")
    difference_path = output_root / "legacy_difference_report.json"
    difference = load_json_strict(difference_path)
    differences = [
        row
        for row in difference["differences"]
        if row.get("difference_id") != "visual-difference:source-native-text-clipping@1"
    ]
    if visual_result == "fail" or not visual_fit["passed"]:
        affected = sorted(
            row["destination"] for row in visual_fit["records"] if not row["pass"]
        )
        differences.append(
            {
                "difference_id": "visual-difference:source-native-text-clipping@1",
                "destinations": affected,
                "classification": "visual difference",
                "difference_reason_code": "presentation_contract_v2_visual_fit_failure",
                "legacy_display_value": None,
                "preview_display_value": None,
                "binding_id": None,
                "source_native_field_id": None,
                "legacy_style_id": None,
                "preview_style_id": None,
                "owned_parity_classification": None,
                "parity_exception_id": None,
                "parity_comparison_digest": None,
            }
        )
    counts: dict[str, int] = {
        key: 0
        for key in (
            "exact parity",
            "normalized presentation parity",
            "accepted source-native semantic correction",
            "expected legacy defect removal",
            "structural difference",
            "visual difference",
            "reviewed layout evolution",
            "mapping defect",
            "unresolved",
        )
    }
    for row in differences:
        counts[row["classification"]] += 1
    difference["differences"] = differences
    difference["difference_count"] = len(differences)
    difference["classification_counts"] = dict(sorted(counts.items()))
    difference["mapping_defect_count"] = counts["mapping defect"]
    difference["unresolved_count"] = counts["unresolved"]
    difference.pop("report_digest", None)
    difference["report_digest"] = hashlib.sha256(canonical_json_bytes(difference)).hexdigest()
    write_deterministic_json(difference_path, difference)
    (output_root / "visual_validation.md").write_text(
        _visual_markdown(
            preview_path=preview_path,
            structural=structural,
            semantic=semantic,
            visual_fit=visual_fit,
            render_images=render_images,
            visual_result=visual_result,
            visual_notes=visual_notes,
        ),
        encoding="utf-8",
        newline="\n",
    )
    manifest = build_preview_manifest(
        output_root=output_root,
        product=product,
        plan=plan,
        legacy_workbook=legacy_workbook,
        artifact_paths=_existing_artifact_paths(output_root, render_images),
        repeated_preview_path=repeat_path,
        design_lock_root=design_lock_root,
    )
    write_deterministic_json(output_root / "preview_manifest.json", manifest)
    return manifest


def build_preview(
    *,
    source_root: Path,
    legacy_workbook: Path,
    design_lock_root: Path,
    output_root: Path,
    refresh_manifest_only: bool,
    render_images: tuple[Path, ...],
    visual_result: str,
    visual_notes: str,
) -> dict:
    if output_root.resolve().is_relative_to(REPOSITORY_ROOT.resolve()):
        raise PromiseProgressWorkbookPreviewError("preview artifacts must be written outside Git")
    product = _build_product(source_root)
    plan = build_promise_progress_workbook_binding_plan(product, design_lock_root=design_lock_root)
    legacy_before = sha256_file(legacy_workbook)
    if legacy_before != EXPECTED_ANF_WORKBOOK_SHA256:
        raise PromiseProgressWorkbookPreviewError(f"legacy ANF workbook differs before preview build: {legacy_before}")
    output_root.mkdir(parents=True, exist_ok=True)
    preview_path = output_root / PREVIEW_NAME
    repeat_path = output_root / REPEAT_NAME

    if not refresh_manifest_only:
        expected_outputs = _existing_artifact_paths(output_root, ()) + [output_root / "preview_manifest.json"]
        existing = [path for path in expected_outputs if path.exists()]
        if existing:
            raise PromiseProgressWorkbookPreviewError(
                f"refusing to overwrite existing preview artifacts: {[str(path) for path in existing]!r}"
            )
        first = materialize_promise_progress_preview(
            product,
            plan,
            legacy_workbook=legacy_workbook,
            output_workbook=preview_path,
            design_lock_root=design_lock_root,
        )
        second = materialize_promise_progress_preview(
            product,
            plan,
            legacy_workbook=legacy_workbook,
            output_workbook=repeat_path,
            design_lock_root=design_lock_root,
        )
        if (
            first["canonical_workbook_content_sha256"] != second["canonical_workbook_content_sha256"]
            or first["target_sheet_semantic_sha256"] != second["target_sheet_semantic_sha256"]
        ):
            raise PromiseProgressWorkbookPreviewError("fresh preview regenerations are not semantically identical")
        structural = validate_preview_structure(
            legacy_workbook=legacy_workbook,
            preview_workbook=preview_path,
            plan=plan,
            design_lock_root=design_lock_root,
        )
        semantic = validate_preview_semantics(product, plan, preview_workbook=preview_path)
        visual_fit = validate_preview_visual_fit(preview_workbook=preview_path, plan=plan)
        difference = build_legacy_difference_report(
            product,
            plan,
            legacy_workbook=legacy_workbook,
            preview_workbook=preview_path,
        )
        trace = build_workbook_trace(product, plan, preview_workbook=preview_path)
        repeat_structural = validate_preview_structure(
            legacy_workbook=legacy_workbook,
            preview_workbook=repeat_path,
            plan=plan,
            design_lock_root=design_lock_root,
        )
        repeat_semantic = validate_preview_semantics(product, plan, preview_workbook=repeat_path)
        repeat_visual_fit = validate_preview_visual_fit(preview_workbook=repeat_path, plan=plan)
        if not structural["passed"] or not semantic["passed"] or not visual_fit["passed"]:
            raise PromiseProgressWorkbookPreviewError("generated preview failed structural, semantic, or visual-fit validation")
        if not repeat_structural["passed"] or not repeat_semantic["passed"] or not repeat_visual_fit["passed"]:
            raise PromiseProgressWorkbookPreviewError("repeat preview failed validation")
        for name, first_result, second_result in (
            ("structure", structural, repeat_structural),
            ("semantics", semantic, repeat_semantic),
            ("visual-fit", visual_fit, repeat_visual_fit),
        ):
            if first_result["validation_digest"] != second_result["validation_digest"]:
                raise PromiseProgressWorkbookPreviewError(f"fresh regeneration {name} digest differs")
        if difference["mapping_defect_count"] or difference["unresolved_count"]:
            raise PromiseProgressWorkbookPreviewError("generated preview contains an unowned parity difference")
        write_deterministic_json(output_root / "binding_plan.json", plan.to_dict())
        write_deterministic_json(output_root / "presentation_contract_v2.json", plan.presentation_contract.to_dict())
        write_deterministic_json(output_root / "workbook_trace.json", trace)
        write_deterministic_json(output_root / "structural_validation.json", structural)
        write_deterministic_json(output_root / "semantic_validation.json", semantic)
        write_deterministic_json(output_root / "visual_fit_validation.json", visual_fit)
        write_deterministic_json(output_root / "legacy_difference_report.json", difference)

    manifest = _refresh_visual_and_manifest(
        product=product,
        plan=plan,
        legacy_workbook=legacy_workbook,
        design_lock_root=design_lock_root,
        output_root=output_root,
        render_images=render_images,
        visual_result=visual_result,
        visual_notes=visual_notes,
    )
    legacy_after = sha256_file(legacy_workbook)
    if legacy_after != legacy_before:
        raise PromiseProgressWorkbookPreviewError("legacy ANF workbook changed during preview generation")
    return {
        "preview": str(preview_path),
        "repeat_preview": str(repeat_path),
        "preview_manifest": str(output_root / "preview_manifest.json"),
        "preview_manifest_sha256": sha256_file(output_root / "preview_manifest.json"),
        "legacy_before_sha256": legacy_before,
        "legacy_after_sha256": legacy_after,
        "binding_plan_sha256": plan.lineage_digest,
        "artifact_count": manifest["artifact_count"],
        "fresh_regeneration": manifest["fresh_regeneration"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the disposable ANF Promise Progress source-native workbook preview.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--legacy-workbook", type=Path, default=DEFAULT_LEGACY_WORKBOOK)
    parser.add_argument("--design-lock-root", type=Path, default=DEFAULT_DESIGN_LOCK_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--refresh-manifest-only", action="store_true")
    parser.add_argument("--render-image", action="append", type=Path, default=[])
    parser.add_argument("--visual-result", choices=("pending", "pass", "fail"), default="pending")
    parser.add_argument(
        "--visual-notes",
        default="Rendered-image inspection is pending; structural and semantic validation are complete.",
    )
    args = parser.parse_args()
    result = build_preview(
        source_root=args.source_root.resolve(),
        legacy_workbook=args.legacy_workbook.resolve(),
        design_lock_root=args.design_lock_root.resolve(),
        output_root=args.output_root.resolve(),
        refresh_manifest_only=args.refresh_manifest_only,
        render_images=tuple(path.resolve() for path in args.render_image),
        visual_result=args.visual_result,
        visual_notes=args.visual_notes,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
