from __future__ import annotations

from datetime import datetime


def create_pipeline_summary(run: dict, fail: bool = False) -> str:
    latest_raw = run.get("latest_record_updated_value", "")
    try:
        latest_time = datetime.fromisoformat(str(latest_raw).replace(" ", "T")).strftime(
            "%Y-%m-%d %Hh:%Mm %Ss"
        )
    except ValueError:
        latest_time = str(latest_raw)

    new_cols = ", ".join(run["new_columns_seen"]) if run.get("new_columns_seen") else "None"

    if fail:
        if run.get("load_status") == "fail":
            fail_reason = run.get("load_status_reason")
        elif run.get("extraction_status") == "fail":
            fail_reason = run.get("extraction_status_reason")
        elif run.get("transform_status") == "fail":
            fail_reason = run.get("transform_status_reason")
        else:
            fail_reason = "Unknown failure."
        return (
            f"*Pipeline `{run.get('table_name')}` failed*:\n"
            f"Records: {run.get('record_count')} (~{run.get('data_size_mb')} MB)\n"
            f"Duration: {run.get('total_duration')}s\n"
            f"Reason: {fail_reason}\n"
            f"Latest record: {latest_time}"
        )

    return (
        f"*Pipeline `{run.get('table_name')}` completed successfully*:\n"
        f"Records: {int(run.get('record_count') or 0)} (~{float(run.get('data_size_mb') or 0):.2f} MB)\n"
        f"Duration: {float(run.get('total_duration') or 0):.2f}s\n"
        f"New column(s): {new_cols}\n"
        f"Latest record: {latest_time}"
    )
