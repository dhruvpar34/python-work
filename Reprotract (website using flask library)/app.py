from __future__ import annotations
import hashlib, json, os, uuid
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session

BASE = Path(__file__).parent
UPLOAD_DIR = BASE / "uploads"
RUN_DIR = BASE / "runs"
UPLOAD_DIR.mkdir(exist_ok=True)
RUN_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "replace-this-in-production")
ALLOWED = {".csv", ".xlsx", ".xls", ".json", ".parquet"}
STAGES = [
    ("1. Ingestion", "Load the uploaded file and capture its initial structure."),
    ("2. Validation", "Check dimensions, duplicate rows, missing values, and column types."),
    ("3. Cleaning", "Normalize column names and remove exact duplicate records."),
    ("4. Transformation", "Convert numeric columns and create a reproducible transformed dataset."),
    ("5. Analysis", "Calculate descriptive statistics and final quality metrics."),
]

def fingerprint(df):
    payload = pd.util.hash_pandas_object(df, index=True).values.tobytes()
    return hashlib.sha256(payload).hexdigest()

def load_frame(path):
    suffix = path.suffix.lower()
    if suffix == ".csv": return pd.read_csv(path)
    if suffix in {".xlsx", ".xls"}: return pd.read_excel(path)
    if suffix == ".json": return pd.read_json(path)
    if suffix == ".parquet": return pd.read_parquet(path)
    raise ValueError("Unsupported file format")

def json_safe(value):
    if isinstance(value, (np.integer,)): return int(value)
    if isinstance(value, (np.floating,)): return None if not np.isfinite(value) else float(value)
    if isinstance(value, (np.bool_,)): return bool(value)
    if pd.isna(value): return None
    return str(value)

def run_pipeline(path, run_id):
    raw = load_frame(path)
    artifacts, metrics = [], []
    current = raw.copy()

    def record(name, frame, details):
        artifacts.append({
            "stage": name, "rows": int(frame.shape[0]), "columns": int(frame.shape[1]),
            "schema": [{"name": str(c), "dtype": str(frame[c].dtype)} for c in frame.columns],
            "fingerprint": fingerprint(frame), "details": details,
            "preview": frame.head(8).replace({np.nan: None}).to_dict(orient="records"),
            # Full stage snapshot enables record/cell-level comparisons.
            "data": frame.replace({np.nan: None}).to_dict(orient="records")
        })

    record(STAGES[0][0], current, {"source_file": path.name, "loaded": True})
    validation = {
        "duplicate_rows": int(current.duplicated().sum()),
        "missing_cells": int(current.isna().sum().sum()),
        "missing_by_column": {str(k): int(v) for k, v in current.isna().sum().items()},
        "numeric_columns": [str(c) for c in current.select_dtypes(include=np.number).columns],
        "non_numeric_columns": [str(c) for c in current.select_dtypes(exclude=np.number).columns],
    }
    record(STAGES[1][0], current, validation)
    current.columns = [str(c).strip().lower().replace(" ", "_") for c in current.columns]
    before = len(current)
    current = current.drop_duplicates().reset_index(drop=True)
    record(STAGES[2][0], current, {"removed_duplicate_rows": int(before-len(current)),
                                   "normalized_column_names": True})
    numeric = current.select_dtypes(include=np.number).columns.tolist()
    for col in numeric:
        current[col] = pd.to_numeric(current[col], errors="coerce")
    record(STAGES[3][0], current, {"numeric_columns_transformed": [str(c) for c in numeric],
                                   "transformation": "numeric coercion; no scaling"})
    summary = {}
    for col in current.select_dtypes(include=np.number).columns:
        s = current[col]
        summary[str(col)] = {"count": int(s.count()), "mean": json_safe(s.mean()),
                             "std": json_safe(s.std()), "min": json_safe(s.min()),
                             "max": json_safe(s.max())}
    analysis = {"numeric_summary": summary, "row_count": int(len(current)),
                "column_count": int(len(current.columns)),
                "missing_cells": int(current.isna().sum().sum()),
                "duplicate_rows": int(current.duplicated().sum())}
    record(STAGES[4][0], current, analysis)
    manifest = {
        "run_id": run_id, "created_at": datetime.now(timezone.utc).isoformat(),
        "file_name": path.name, "file_size_bytes": path.stat().st_size,
        "pipeline": "REPROTRACE five-stage demo pipeline",
        "tolerances": {"atol": 1e-8, "rtol": 1e-5}, "stages": artifacts
    }
    (RUN_DIR / f"{run_id}.json").write_text(json.dumps(manifest, indent=2, default=json_safe))
    return manifest

def get_runs():
    runs=[]
    for p in sorted(RUN_DIR.glob("*.json"), key=lambda x:x.stat().st_mtime, reverse=True):
        try: runs.append(json.loads(p.read_text()))
        except Exception: continue
    return runs

def _is_missing(value):
    return value is None or (isinstance(value, float) and np.isnan(value))

def _cell_equal(left, right, atol, rtol):
    if _is_missing(left) and _is_missing(right):
        return True, "same"
    if _is_missing(left) or _is_missing(right):
        return False, "missing-value change"
    # Numeric values are tolerance-aware at every stage, not only in summaries.
    if isinstance(left, (int, float, np.integer, np.floating)) and not isinstance(left, bool) and \
       isinstance(right, (int, float, np.integer, np.floating)) and not isinstance(right, bool):
        equal = bool(np.isclose(float(left), float(right), atol=atol, rtol=rtol, equal_nan=True))
        return equal, "numeric"
    equal = str(left) == str(right)
    return equal, "value"

def compare_stage(a, b, atol, rtol):
    changes=[]
    if a["rows"] != b["rows"]: changes.append(f"Row count: {a['rows']} → {b['rows']}")
    if a["columns"] != b["columns"]: changes.append(f"Column count: {a['columns']} → {b['columns']}")
    left_cols=[x["name"] for x in a.get("schema", [])]
    right_cols=[x["name"] for x in b.get("schema", [])]
    if left_cols != right_cols: changes.append("Column names or order differ.")
    left_types={x["name"]:x["dtype"] for x in a.get("schema", [])}
    right_types={x["name"]:x["dtype"] for x in b.get("schema", [])}
    type_changes=[c for c in set(left_types)&set(right_types) if left_types[c] != right_types[c]]
    if type_changes: changes.append("Data type changes: " + ", ".join(sorted(type_changes)))

    left_rows=a.get("data", a.get("preview", []))
    right_rows=b.get("data", b.get("preview", []))
    common_cols=[c for c in left_cols if c in right_cols]
    max_rows=max(len(left_rows),len(right_rows))
    row_diffs=[]; changed_cells=0; numeric_outside=0; within_tolerance=0
    added_rows=max(0,len(right_rows)-len(left_rows)); removed_rows=max(0,len(left_rows)-len(right_rows))
    for idx in range(max_rows):
        lrow=left_rows[idx] if idx < len(left_rows) else None
        rrow=right_rows[idx] if idx < len(right_rows) else None
        if lrow is None or rrow is None:
            row_diffs.append({"row":idx+1,"column":"(row)","baseline":"<missing>" if lrow is None else "present",
                              "candidate":"present" if rrow is not None else "<missing>","kind":"added/removed row"})
            continue
        for col in common_cols:
            lv=lrow.get(col); rv=rrow.get(col)
            equal,kind=_cell_equal(lv,rv,atol,rtol)
            if not equal:
                changed_cells += 1
                if kind == "numeric": numeric_outside += 1
                if len(row_diffs)<100:
                    row_diffs.append({"row":idx+1,"column":col,"baseline":lv,"candidate":rv,"kind":kind})
            elif kind == "numeric" and lv is not None and rv is not None and float(lv) != float(rv):
                within_tolerance += 1

    missing_left=sorted(set(left_cols)-set(right_cols)); missing_right=sorted(set(right_cols)-set(left_cols))
    if missing_left: changes.append("Columns removed: " + ", ".join(missing_left))
    if missing_right: changes.append("Columns added: " + ", ".join(missing_right))
    if added_rows: changes.append(f"Added rows: {added_rows}")
    if removed_rows: changes.append(f"Removed rows: {removed_rows}")
    if changed_cells: changes.append(f"Changed cells outside tolerance: {changed_cells}")
    numeric_note=(f"Compared numeric cells across this stage using atol={atol:g}, rtol={rtol:g}. "
                  f"{numeric_outside} numeric cell(s) outside tolerance; {within_tolerance} numeric cell(s) varied but remained within tolerance.")
    # Metadata is shown separately so incidental values (for example source filenames)
    # do not falsely become the earliest data divergence.
    da,db=a.get("details",{}),b.get("details",{})
    metadata_note="Stage metadata differs; inspect each run manifest for context." if da != db else "Stage metadata matches."
    if changes or changed_cells or added_rows or removed_rows:
        status="Different"
    else:
        status="Identical" if a.get("fingerprint")==b.get("fingerprint") else "Within tolerance"
    return {"status":status,"changes":changes,
            "numeric_note":numeric_note,
            "metadata_note":metadata_note,
            "row_diff_count":changed_cells+added_rows+removed_rows,
            "row_diffs":row_diffs,
            "row_diff_truncated":changed_cells+added_rows+removed_rows>len(row_diffs),
            "added_rows":added_rows,"removed_rows":removed_rows}

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", runs=get_runs(), stages=STAGES)

@app.route("/upload", methods=["POST"])
def upload():
    uploaded=request.files.getlist("files")
    if not uploaded or not any(f.filename for f in uploaded):
        flash("Choose at least one file to upload.", "error"); return redirect(url_for("index"))
    successes=0
    for f in uploaded:
        name=Path(f.filename).name
        suffix=Path(name).suffix.lower()
        if suffix not in ALLOWED:
            flash(f"Skipped {name}: unsupported format.", "error"); continue
        run_id=uuid.uuid4().hex[:12]
        dest=UPLOAD_DIR / f"{run_id}_{name}"
        f.save(dest)
        try:
            run_pipeline(dest, run_id); successes+=1
        except Exception as e:
            dest.unlink(missing_ok=True)
            flash(f"Could not process {name}: {e}", "error")
    if successes: flash(f"Successfully processed {successes} file(s) through all five stages.", "success")
    return redirect(url_for("index"))

@app.route("/compare", methods=["POST"])
def compare():
    ids=request.form.getlist("run_ids")
    try:
        atol=max(0.0,float(request.form.get("atol","1e-8")))
        rtol=max(0.0,float(request.form.get("rtol","1e-5")))
    except ValueError:
        flash("Tolerance values must be numbers.", "error"); return redirect(url_for("index"))
    if len(ids)<2:
        flash("Select at least two runs to compare.", "error"); return redirect(url_for("index"))
    all_runs={r["run_id"]:r for r in get_runs()}
    selected=[all_runs[i] for i in ids if i in all_runs]
    if len(selected)<2:
        flash("One or more selected runs could not be found.", "error"); return redirect(url_for("index"))
    # Pairwise against first selected run, with stage-by-stage earliest divergence.
    baseline=selected[0]
    comparisons=[]
    for other in selected[1:]:
        stages=[]
        earliest=None
        for left,right in zip(baseline["stages"],other["stages"]):
            result=compare_stage(left,right,atol,rtol)
            stages.append({"name":left["stage"],"left":left,"right":right,**result})
            if earliest is None and result["status"]=="Different": earliest=left["stage"]
        comparisons.append({"baseline":baseline,"other":other,"stages":stages,
                            "earliest":earliest or "No meaningful divergence detected"})
    return render_template("comparison.html", comparisons=comparisons, selected=selected, atol=atol, rtol=rtol)

@app.route("/run/<run_id>")
def run_detail(run_id):
    p=RUN_DIR/f"{run_id}.json"
    if not p.exists(): flash("Run not found.","error"); return redirect(url_for("index"))
    return render_template("run.html", run=json.loads(p.read_text()), stages=STAGES)

@app.route("/report/<run_id>")
def report(run_id):
    p=RUN_DIR/f"{run_id}.json"
    if not p.exists(): return "Run not found",404
    return send_file(p, as_attachment=True, download_name=f"reprotrace_{run_id}_manifest.json")

@app.route("/health")
def health(): return {"status":"ok","service":"REPROTRACE Flask"}

if __name__ == "__main__":
    app.run(debug=True)
