# REPROTRACE — Flask pipeline observability demo

A Flask-based web application (no Streamlit) for uploading multiple datasets, executing each through an explicitly instrumented five-stage pipeline, reviewing execution manifests, and comparing two or more runs.

## Five instrumented stages
1. **Ingestion** — load source data and capture initial schema/fingerprint.
2. **Validation** — measure duplicates, missing cells, and data types.
3. **Cleaning** — normalize column names and remove exact duplicate rows.
4. **Transformation** — coerce numeric columns and record transformation metadata.
5. **Analysis** — calculate descriptive statistics and final quality metrics.

Each stage records row/column counts, schema, SHA-256 fingerprint, stage metadata, a small preview, and a full row snapshot for comparison. Run manifests are JSON files in `runs/`.

## Run locally
```bash
python -m venv .venv
# Windows: .venv\\Scripts\\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python app.py
```
Open http://127.0.0.1:5000

## Use
- Upload multiple CSV, XLSX, XLS, JSON, or Parquet files at once.
- Select any two or more runs from the execution history.
- Set absolute (`atol`) and relative (`rtol`) tolerances and compare.
- The comparison view reports the earliest stage with a differing artifact fingerprint, schema, dimensions, or metadata.
- Open a run to inspect all five stage records and download its JSON manifest.

## Notes / limitations
This is a runnable project starter and demonstration pipeline, not a sandbox for executing arbitrary user code. The five transformations are intentionally explicit and deterministic. Earliest divergence is the first stage whose artifact fingerprint differs; metadata differences are shown as evidence. Numeric tolerance is currently applied to the numeric summary metrics at the Analysis stage. For production use, add authentication, upload size limits, malware scanning, isolated storage, robust type-aware row-level diffs, code/environment fingerprints, Git commit capture, retention policies, and controlled reruns against versioned pipeline code/configuration. A true controlled rerun requires a versioned pipeline implementation and immutable input artifacts; this starter does not claim to revert Git changes automatically. Row-level alignment currently uses row position, so datasets with nondeterministic row ordering should be sorted by a stable domain key before upload or extended with a configurable primary-key matcher. Full stage snapshots increase manifest storage use; production deployments should add retention and size controls.
