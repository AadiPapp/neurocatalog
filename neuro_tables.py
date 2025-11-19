import os
from pathlib import Path
from typing import Iterator
from datetime import datetime

import polars as pl
from pynwb import NWBHDF5IO
from datarepo import table, NlkDataFrame

DATA_ROOT = Path(os.getenv("NEURO_DATA_ROOT", ""))

@table(
    description="Metadata for every NWB file. Auto-discovered or dummy data for demo.",
    unique_columns=["session_id"],
)
def sessions() -> NlkDataFrame:
    nwb_paths = list(DATA_ROOT.rglob("*.nwb")) if DATA_ROOT else []
    
    if not nwb_paths:
        # Dummy data so the site is beautiful even with no files
        return pl.LazyFrame([
            {"session_id": "demo_001", "subject_id": "subj01", "session_start_time": datetime(2025, 1, 1), "session_description": "Reaching task with Utah array", "lab": "Example Lab", "n_units": 1247, "file_size_gb": 4.2},
            {"session_id": "demo_002", "subject_id": "subj02", "session_start_time": datetime(2025, 1, 15), "session_description": "BCI cursor control", "lab": "Example Lab", "n_units": 892, "file_size_gb": 6.8},
            {"session_id": "demo_003", "subject_id": "subj01", "session_start_time": datetime(2025, 2, 1), "session_description": "Speech decoding pilot", "lab": "Example Lab", "n_units": 2048, "file_size_gb": 12.1},
        ])
    
    rows = []
    for path in nwb_paths:
        try:
            with NWBHDF5IO(path, "r") as io:
                nwb = io.read()
                rows.append({
                    "session_id": nwb.identifier,
                    "subject_id": nwb.subject.subject_id if nwb.subject else "unknown",
                    "session_start_time": nwb.session_start_time,
                    "session_description": nwb.session_description or "",
                    "lab": nwb.lab or "",
                    "n_units": len(nwb.units) if nwb.units else 0,
                    "file_size_gb": round(path.stat().st_size / 1e9, 3),
                })
        except Exception:
            continue
    return pl.LazyFrame(rows)


@table(
    description="All units across all sessions (streams millions of rows lazily)."
)
def units() -> Iterator[NlkDataFrame]:
    if not DATA_ROOT or not list(DATA_ROOT.rglob("*.nwb")):
        # Dummy units for demo
        yield pl.DataFrame([
            {"unit_id": 42, "session_id": "demo_001", "subject_id": "subj01", "spike_times": [0.1, 0.42, 1.337, 2.718], "waveform_mean": [100, -50, 200], "quality": "good"},
            {"unit_id": 1337, "session_id": "demo_001", "subject_id": "subj01", "spike_times": [0.01, 0.69, 1.618], "quality": "excellent"},
        ])
        return

    for path in DATA_ROOT.rglob("*.nwb"):
        try:
            with NWBHDF5IO(path, "r") as io:
                nwb = io.read()
                if not nwb.units:
                    continue
                df = nwb.units.to_dataframe()
                df = df.with_columns([
                    pl.lit(nwb.identifier).alias("session_id"),
                    pl.lit(str(path)).alias("file_path"),
                ])
                yield df
        except Exception:
            continue
