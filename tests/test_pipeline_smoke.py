from pathlib import Path

import pandas as pd

from uk_travel_pipeline.config import MatrixConfig
from uk_travel_pipeline.matrix import run_matrices


def test_legacy_output_written_only_when_enabled(tmp_path: Path):
    adjusted = tmp_path / "adjusted.parquet"
    df = pd.DataFrame(
        {
            "origin_msoa": ["A"],
            "destination_msoa": ["B"],
            "mode_of_transport": ["ROAD"],
            "time_period": ["AM_peak"],
            "weekend_flag": [0],
            "days_used": [1],
            "volume_adj": [10],
        }
    )
    df.to_parquet(adjusted, index=False)

    outputs = tmp_path / "outputs"
    legacy = tmp_path / "output"
    cfg = MatrixConfig(adjusted_parquet=adjusted, purpose_parquet=None, outputs_root=outputs, modes=("ROAD",))

    run_matrices(cfg, legacy_output_root=None)
    assert (outputs / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted.csv").exists()
    assert not (legacy / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted.csv").exists()

    run_matrices(cfg, legacy_output_root=legacy)
    assert (legacy / "matrices" / "typical_week_by_mode" / "OD_matrix_ROAD_adjusted.csv").exists()
