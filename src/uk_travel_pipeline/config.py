from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_BT_PARQUET = Path("data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet")
DEFAULT_MSOA_GEOJSON = Path("data/raw/geo/Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V3_4916445166053426.geojson")
DEFAULT_NTS_FILE = Path("data/raw/nts/nts9916.ods")
DEFAULT_MSOA_REGION_LOOKUP = Path("data/raw/lookups/msoa_to_region.csv")
DEFAULT_ADJUSTED_PARQUET = Path("data/processed/reassign/trips_adjusted.parquet")
DEFAULT_OUTPUTS_ROOT = Path("outputs")
DEFAULT_LEGACY_OUTPUT_ROOT = Path("output")

DEFAULT_MODES = ("ROAD", "RAIL", "WALKING", "SUBWAY")
DEFAULT_YEAR = 2024
DEFAULT_REGION = None
DEFAULT_FACTOR_MIN = 0.01
DEFAULT_FACTOR_MAX = 100.0


@dataclass(frozen=True)
class ReassignConfig:
    bt_parquet: Path = DEFAULT_BT_PARQUET
    msoa_filter_csv: Path | None = None
    msoa_region_lookup_csv: Path | None = None
    msoa_geojson: Path = DEFAULT_MSOA_GEOJSON
    nts_file: Path = DEFAULT_NTS_FILE
    adjusted_parquet: Path = DEFAULT_ADJUSTED_PARQUET
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT
    year: int = DEFAULT_YEAR
    region: str | None = DEFAULT_REGION
    factor_min: float = DEFAULT_FACTOR_MIN
    factor_max: float = DEFAULT_FACTOR_MAX


@dataclass(frozen=True)
class MatrixConfig:
    adjusted_parquet: Path = DEFAULT_ADJUSTED_PARQUET
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT
    modes: tuple[str, ...] = DEFAULT_MODES


@dataclass(frozen=True)
class PipelineConfig:
    reassign: ReassignConfig
    matrix: MatrixConfig
    legacy_output: bool = False
    legacy_output_root: Path = DEFAULT_LEGACY_OUTPUT_ROOT
