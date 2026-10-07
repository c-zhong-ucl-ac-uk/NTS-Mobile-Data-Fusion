from __future__ import annotations

import re
from pathlib import Path
from tempfile import TemporaryDirectory

import dask
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.csv as arrow_csv

from .io import assert_files_exist, ensure_dir


_PART_FILENAME = re.compile(r"part-\d+\.csv")


def _write_csv_partition(partition: pd.DataFrame, output_path: Path) -> Path:
    table = pa.Table.from_pandas(partition, preserve_index=False)
    arrow_csv.write_csv(table, output_path, write_options=arrow_csv.WriteOptions(include_header=True))
    return output_path


def export_adjusted_trip_csv(adjusted_parquet: Path, output_dir: Path) -> list[Path]:
    """Export trip records as UTF-8 CSV partitions, each with its own header.

    Numeric ``part-*.csv`` names are reserved for this export. A successful rerun
    replaces these files and removes obsolete partitions; other files are kept.
    New CSVs are staged before replacing any previous export. Records remain
    partitioned throughout, without loading the complete dataset into pandas.
    """
    assert_files_exist([adjusted_parquet])
    if adjusted_parquet.is_dir() and output_dir.resolve().is_relative_to(adjusted_parquet.resolve()):
        raise ValueError("CSV output directory must be outside the source parquet dataset.")

    trips = dd.read_parquet(adjusted_parquet)
    ensure_dir(output_dir)
    previous_parts = {
        path for path in output_dir.iterdir() if path.is_file() and _PART_FILENAME.fullmatch(path.name)
    }

    with TemporaryDirectory(prefix=".adjusted-trip-csv-", dir=output_dir) as temporary_dir:
        writes = [
            dask.delayed(_write_csv_partition)(partition, Path(temporary_dir) / f"part-{index:05d}.csv")
            for index, partition in enumerate(trips.to_delayed())
        ]
        written = dask.compute(*writes)
        exported = []
        for filename in sorted(written):
            staged = Path(filename)
            destination = output_dir / staged.name
            staged.replace(destination)
            exported.append(destination)

        for stale in previous_parts.difference(exported):
            stale.unlink()

    return exported
