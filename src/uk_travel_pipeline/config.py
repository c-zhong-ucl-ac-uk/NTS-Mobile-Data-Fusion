from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_BT_PARQUET = Path("data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet")
DEFAULT_MSOA_GEOJSON = Path("data/raw/geo/Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V3_4916445166053426.geojson")
DEFAULT_NTS_FILE = Path("data/raw/nts/nts9916.ods")
DEFAULT_MSOA_REGION_LOOKUP = Path("data/raw/lookups/msoa_to_region.csv")
DEFAULT_ADJUSTED_PARQUET = Path("data/processed/reassign/trips_adjusted.parquet")
DEFAULT_PURPOSE_PARQUET = Path("data/processed/reassign/trips_adjusted_by_purpose.parquet")
DEFAULT_OUTPUTS_ROOT = Path("outputs")
DEFAULT_LOCAL_AUTHORITY_OUTPUT_DIR = DEFAULT_OUTPUTS_ROOT / "local_authority_use_cases"
DEFAULT_LEGACY_OUTPUT_ROOT = Path("output")
DEFAULT_POP_LSOA_INTERNAL = Path("data/raw/trip_production_calculation_code/pop_lad_gb/pop_id.csv")
DEFAULT_TFN_AREA_TYPE_LSOA = Path("data/raw/trip_production_calculation_code/tfn_area_type_lsoa21.csv")
DEFAULT_LSOA_MSOA_LOOKUP = Path("data/raw/lookups/OA21_LSOA21_MSOA21_LAD21_EW_V3.csv")
DEFAULT_NTS_MODE_TIME_SPLIT = (
    Path("data/raw/trip_production_calculation_code/01202 - NTS Trip Rates v23.0/mode_time_split_production_hb_fr_reg.csv")
)
DEFAULT_PURPOSES_CSV = Path("data/raw/trip_production_calculation_code/01202 - NTS Trip Rates v23.0/data_definition/purposes.csv")
DEFAULT_NTS0502_PERIOD_PURPOSE_CSV = Path("data/raw/nts/nts0502_period_purpose_controls.csv")

DEFAULT_MODES = ("ROAD", "RAIL", "WALKING", "SUBWAY")
DEFAULT_MODES_WITH_ROAD_SPLIT = ("CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS", "RAIL", "WALKING", "SUBWAY")
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
    purpose_parquet: Path = DEFAULT_PURPOSE_PARQUET
    pop_lsoa_internal_csv: Path = DEFAULT_POP_LSOA_INTERNAL
    tfn_area_type_lsoa_csv: Path = DEFAULT_TFN_AREA_TYPE_LSOA
    lsoa_msoa_lookup_csv: Path = DEFAULT_LSOA_MSOA_LOOKUP
    nts_mode_time_split_csv: Path = DEFAULT_NTS_MODE_TIME_SPLIT
    purposes_csv: Path = DEFAULT_PURPOSES_CSV
    nts0502_period_purpose_csv: Path | None = DEFAULT_NTS0502_PERIOD_PURPOSE_CSV
    estimate_purpose: bool = True
    purpose_calibration: str = "mode_time_split"
    split_road_mode: bool = False
    constrain_mode_time_share: bool = True
    apply_child_origin_uplift: bool = True
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT
    year: int = DEFAULT_YEAR
    region: str | None = DEFAULT_REGION
    factor_min: float = DEFAULT_FACTOR_MIN
    factor_max: float = DEFAULT_FACTOR_MAX


@dataclass(frozen=True)
class MatrixConfig:
    adjusted_parquet: Path = DEFAULT_ADJUSTED_PARQUET
    purpose_parquet: Path | None = DEFAULT_PURPOSE_PARQUET
    nts_mode_time_split_csv: Path | None = DEFAULT_NTS_MODE_TIME_SPLIT
    purposes_csv: Path | None = DEFAULT_PURPOSES_CSV
    outputs_root: Path = DEFAULT_OUTPUTS_ROOT
    modes: tuple[str, ...] = DEFAULT_MODES
    purpose_only: bool = False
    write_qa_summary: bool = True


@dataclass(frozen=True)
class LocalAuthorityVisualConfig:
    input_dir: Path = DEFAULT_LOCAL_AUTHORITY_OUTPUT_DIR
    output_dir: Path = DEFAULT_LOCAL_AUTHORITY_OUTPUT_DIR / "visualisations"
    msoa_geojson: Path = DEFAULT_MSOA_GEOJSON
    top_n: int = 12


@dataclass(frozen=True)
class PipelineConfig:
    reassign: ReassignConfig
    matrix: MatrixConfig
    legacy_output: bool = False
    legacy_output_root: Path = DEFAULT_LEGACY_OUTPUT_ROOT
