# Standard Template Shell Visual Gap Audit

Generated at: 2026-07-26T13:38:09.442334+00:00

Preview mode: openpyxl/static only. These PNGs are contact sheets for structural review, not Excel/COM-rendered visual PASS artifacts.

## Summary

- Shell: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\standard_stock_model_template.xlsx`
- ANF lab source: `C:\Users\Jibbe\Aktier\Code\.worktrees\refactor-new-ticker-template-engine\templates\lab\ANF_template_lab.xlsx`
- Shell validator status: `PASS`
- Visually complete sheets: 4/10

## Contact Sheets

- standard_shell_contact_sheet: `C:\Users\Jibbe\AppData\Local\Temp\standard-template-audit-u4m9ge8r\previews\standard_shell_contact_sheet.png`
- anf_template_lab_contact_sheet: `C:\Users\Jibbe\AppData\Local\Temp\standard-template-audit-u4m9ge8r\previews\anf_template_lab_contact_sheet.png`
- shell_vs_anf_contact_sheet: `C:\Users\Jibbe\AppData\Local\Temp\standard-template-audit-u4m9ge8r\previews\shell_vs_anf_contact_sheet.png`

## Sheet Reports

### SUMMARY

- Used range: shell `A1:F45` vs ANF lab `A1:F45`
- Non-empty cells: shell `41` vs ANF lab `85`
- Static/template labels: shell `33` vs ANF lab `48`
- Row labels: shell `28` vs ANF lab `28`
- Formulas: shell `8` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `8` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `28` vs ANF lab `26`
- Hidden columns: `none`
- Freeze panes: `A2`
- Row height similarity: `0.844`; column width similarity: `1.0`
- Writable cells blank/nonblank: `31` / `0`
- Visually complete: `False`
- Gaps:
  - `should_keep_style_or_layout` P2: Row-height or column-width similarity is below the rich-shell threshold.

### Valuation

- Used range: shell `A1:AI271` vs ANF lab `A1:AI261`
- Non-empty cells: shell `1230` vs ANF lab `1748`
- Static/template labels: shell `442` vs ANF lab `592`
- Row labels: shell `142` vs ANF lab `163`
- Formulas: shell `788` vs ANF lab `75`
- Formula/helper cells outside writable zones: shell `788` vs ANF lab `75`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `319` vs ANF lab `422`
- Hidden columns: `AI`
- Freeze panes: `B7`
- Row height similarity: `0.982`; column width similarity: `1.0`
- Writable cells blank/nonblank: `649` / `0`
- Visually complete: `True`
- Gaps: none material after clearing company-specific value zones.

### BS_Segments

- Used range: shell `A1:M78` vs ANF lab `A1:I74`
- Non-empty cells: shell `235` vs ANF lab `358`
- Static/template labels: shell `67` vs ANF lab `68`
- Row labels: shell `60` vs ANF lab `61`
- Formulas: shell `168` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `168` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `4` vs ANF lab `4`
- Hidden columns: `none`
- Freeze panes: `B8`
- Row height similarity: `0.97`; column width similarity: `0.692`
- Writable cells blank/nonblank: `482` / `0`
- Visually complete: `False`
- Gaps:
  - `should_keep_style_or_layout` P2: Row-height or column-width similarity is below the rich-shell threshold.

### Operating_Drivers

- Used range: shell `A1:N125` vs ANF lab `A1:N115`
- Non-empty cells: shell `45` vs ANF lab `540`
- Static/template labels: shell `45` vs ANF lab `112`
- Row labels: shell `39` vs ANF lab `106`
- Formulas: shell `0` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `0` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `61` vs ANF lab `61`
- Hidden columns: `none`
- Freeze panes: `A2`
- Row height similarity: `0.939`; column width similarity: `1.0`
- Writable cells blank/nonblank: `999` / `0`
- Visually complete: `True`
- Gaps: none material after clearing company-specific value zones.

### {ticker}_Investment_Case

- Used range: shell `A1:K233` vs ANF lab `A1:K233`
- Non-empty cells: shell `235` vs ANF lab `807`
- Static/template labels: shell `129` vs ANF lab `425`
- Row labels: shell `100` vs ANF lab `191`
- Formulas: shell `106` vs ANF lab `208`
- Formula/helper cells outside writable zones: shell `106` vs ANF lab `167`
- ANF formulas cleared because they were inside writable value zones: `41`
- Merges: shell `270` vs ANF lab `295`
- Hidden columns: `none`
- Freeze panes: `A4`
- Row height similarity: `1.0`; column width similarity: `1.0`
- Writable cells blank/nonblank: `290` / `0`
- Visually complete: `False`
- Gaps:
  - `should_keep_formula` P2: Formula/helper count outside writable zones is lower than the ANF lab contract. shell=106 lab=167.

### Quarter_Notes_UI

- Used range: shell `A1:O353` vs ANF lab `A1:O353`
- Non-empty cells: shell `276` vs ANF lab `1054`
- Static/template labels: shell `276` vs ANF lab `1020`
- Row labels: shell `144` vs ANF lab `300`
- Formulas: shell `0` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `0` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `1006` vs ANF lab `1006`
- Hidden columns: `none`
- Freeze panes: `A2`
- Row height similarity: `1.0`; column width similarity: `1.0`
- Writable cells blank/nonblank: `34` / `0`
- Visually complete: `True`
- Gaps: none material after clearing company-specific value zones.

### Promise_Progress_UI

- Used range: shell `A1:O115` vs ANF lab `A1:O102`
- Non-empty cells: shell `124` vs ANF lab `654`
- Static/template labels: shell `124` vs ANF lab `158`
- Row labels: shell `25` vs ANF lab `52`
- Formulas: shell `0` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `0` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `66` vs ANF lab `66`
- Hidden columns: `M, N, O`
- Freeze panes: `A2`
- Row height similarity: `0.982`; column width similarity: `1.0`
- Writable cells blank/nonblank: `903` / `0`
- Visually complete: `True`
- Gaps: none material after clearing company-specific value zones.

### QA_Log

- Used range: shell `A1:Z5000` vs ANF lab `A1:K1256`
- Non-empty cells: shell `12` vs ANF lab `11320`
- Static/template labels: shell `12` vs ANF lab `11`
- Row labels: shell `1` vs ANF lab `1`
- Formulas: shell `0` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `0` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `0` vs ANF lab `0`
- Hidden columns: `none`
- Freeze panes: `A2`
- Row height similarity: `0.0`; column width similarity: `0.0`
- Writable cells blank/nonblank: `129974` / `0`
- Visually complete: `False`
- Gaps:
  - `should_keep_style_or_layout` P2: Row-height or column-width similarity is below the rich-shell threshold.

### Needs_Review

- Used range: shell `A1:Z5000` vs ANF lab `A1:M51`
- Non-empty cells: shell `11` vs ANF lab `577`
- Static/template labels: shell `11` vs ANF lab `13`
- Row labels: shell `1` vs ANF lab `1`
- Formulas: shell `0` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `0` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `0` vs ANF lab `0`
- Hidden columns: `none`
- Freeze panes: `A2`
- Row height similarity: `0.0`; column width similarity: `0.154`
- Writable cells blank/nonblank: `129974` / `0`
- Visually complete: `False`
- Gaps:
  - `should_keep_style_or_layout` P2: Row-height or column-width similarity is below the rich-shell threshold.

### QA_Checks

- Used range: shell `A1:Z5000` vs ANF lab `A1:X776`
- Non-empty cells: shell `9` vs ANF lab `8157`
- Static/template labels: shell `9` vs ANF lab `24`
- Row labels: shell `1` vs ANF lab `1`
- Formulas: shell `0` vs ANF lab `0`
- Formula/helper cells outside writable zones: shell `0` vs ANF lab `0`
- ANF formulas cleared because they were inside writable value zones: `0`
- Merges: shell `0` vs ANF lab `0`
- Hidden columns: `none`
- Freeze panes: `A2`
- Row height similarity: `0.0`; column width similarity: `0.625`
- Writable cells blank/nonblank: `129974` / `0`
- Visually complete: `False`
- Gaps:
  - `should_keep_style_or_layout` P2: Row-height or column-width similarity is below the rich-shell threshold.
