from pathlib import Path

import pandas as pd
import pytest

from uk_travel_pipeline import cli


def test_main_auto_uses_default_msoa_region_lookup(tmp_path: Path, monkeypatch):
    default_lookup = tmp_path / "data" / "raw" / "lookups" / "msoa_to_region.csv"
    default_lookup.parent.mkdir(parents=True, exist_ok=True)
    default_lookup.write_text("MSOA21CD,Region of residence\nE02000001,London\n", encoding="utf-8")

    calls = {}

    def fake_run_reassign(cfg, legacy_output_root=None):
        calls["cfg"] = cfg
        return cfg.adjusted_parquet

    def fake_run_matrices(cfg, legacy_output_root=None):
        calls["matrix_cfg"] = cfg
        return cfg.outputs_root

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_reassign", fake_run_reassign)
    monkeypatch.setattr(cli, "run_matrices", fake_run_matrices)
    monkeypatch.setattr("sys.argv", ["uk-travel-pipeline", "run"])

    cli.main()

    assert calls["cfg"].msoa_region_lookup_csv == Path("data/raw/lookups/msoa_to_region.csv")
    assert calls["cfg"].constrain_mode_time_share is True


def test_main_can_skip_mode_time_constraint(tmp_path: Path, monkeypatch):
    calls = {}

    def fake_run_reassign(cfg, legacy_output_root=None):
        calls["cfg"] = cfg
        return cfg.adjusted_parquet

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_reassign", fake_run_reassign)
    monkeypatch.setattr("sys.argv", ["uk-travel-pipeline", "reassign", "--skip-mode-time-constraint"])

    cli.main()

    assert calls["cfg"].constrain_mode_time_share is False


@pytest.mark.parametrize("command", ["run", "matrices"])
def test_skip_purpose_estimation_excludes_purpose_input_from_matrices(tmp_path: Path, monkeypatch, command):
    calls = {}
    adjusted = Path("data/processed/reassign/trips_adjusted_step7.parquet")
    outputs = Path("outputs_step7")

    def fake_run_reassign(cfg, legacy_output_root=None):
        calls["reassign_cfg"] = cfg
        return cfg.adjusted_parquet

    def fake_run_matrices(cfg, legacy_output_root=None):
        calls["matrix_cfg"] = cfg

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_reassign", fake_run_reassign)
    monkeypatch.setattr(cli, "run_matrices", fake_run_matrices)
    monkeypatch.setattr(
        "sys.argv",
        [
            "uk-travel-pipeline",
            command,
            "--split-road-mode",
            "--skip-mode-time-constraint",
            "--skip-purpose-estimation",
            "--outputs-root",
            str(outputs),
            "--adjusted-parquet",
            str(adjusted),
        ],
    )

    cli.main()

    assert calls["matrix_cfg"].purpose_parquet is None
    assert calls["matrix_cfg"].adjusted_parquet == adjusted
    assert calls["matrix_cfg"].outputs_root == outputs
    assert "PRIVATE_CAR" in calls["matrix_cfg"].modes
    assert "ROAD" not in calls["matrix_cfg"].modes
    if command == "run":
        assert calls["reassign_cfg"].estimate_purpose is False
        assert calls["reassign_cfg"].constrain_mode_time_share is False
        assert calls["reassign_cfg"].adjusted_parquet == adjusted
        assert calls["reassign_cfg"].outputs_root == outputs
    else:
        assert "reassign_cfg" not in calls


@pytest.mark.parametrize("skip_purpose", [True, False])
def test_matrices_cli_controls_existing_purpose_outputs(tmp_path: Path, monkeypatch, skip_purpose):
    monkeypatch.chdir(tmp_path)
    adjusted = Path("data/processed/reassign/trips_adjusted_step7.parquet")
    purpose = cli.DEFAULT_PURPOSE_PARQUET
    adjusted.parent.mkdir(parents=True)
    base = pd.DataFrame(
        {
            "origin_msoa": ["A", "A"],
            "destination_msoa": ["B", "B"],
            "mode_of_transport": ["PRIVATE_CAR", "PRIVATE_CAR"],
            "time_period": ["AM_peak", "Inter_peak"],
            "weekend_flag": [0, 1],
            "days_used": [2, 2],
            "volume_adj": [20.0, 6.0],
        }
    )
    base.to_parquet(adjusted, index=False)
    stale_purpose = base.assign(
        purpose=1,
        purpose_desc="Commuting",
        volume_adj_purpose=[2000.0, 600.0],
    )
    stale_purpose.to_parquet(purpose, index=False)
    purpose_bytes = purpose.read_bytes()
    outputs = Path("outputs_step7")
    args = [
        "uk-travel-pipeline",
        "matrices",
        "--adjusted-parquet",
        str(adjusted),
        "--outputs-root",
        str(outputs),
        "--modes",
        "PRIVATE_CAR",
    ]
    if skip_purpose:
        args.append("--skip-purpose-estimation")
    monkeypatch.setattr("sys.argv", args)

    cli.main()

    matrix = pd.read_csv(outputs / "matrices/typical_week_by_mode/OD_matrix_PRIVATE_CAR_adjusted.csv")
    assert matrix.loc[matrix["origin_msoa"] == "A", "B"].item() == 56.0
    mode_summary = pd.read_csv(outputs / "qa/all_week_mode.csv")
    assert mode_summary["trips"].tolist() == [26.0]
    if skip_purpose:
        assert not list(outputs.rglob("*purpose*.csv"))
        assert {path.name for path in (outputs / "qa").glob("*.csv")} == {
            "all_week_mode.csv",
            "am_weekday_mode.csv",
        }
    else:
        purpose_matrix = outputs / "matrices/typical_week_by_mode/OD_matrix_PRIVATE_CAR_adjusted_by_purpose1.csv"
        assert purpose_matrix.exists()
        purpose_summary = pd.read_csv(outputs / "qa/all_week_purpose.csv")
        assert purpose_summary["trips"].tolist() == [2600.0]
    assert purpose.read_bytes() == purpose_bytes
    assert not Path("outputs").exists()


def test_main_can_skip_child_origin_uplift(tmp_path: Path, monkeypatch):
    calls = {}

    def fake_run_reassign(cfg, legacy_output_root=None):
        calls["cfg"] = cfg
        return cfg.adjusted_parquet

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_reassign", fake_run_reassign)
    monkeypatch.setattr("sys.argv", ["uk-travel-pipeline", "reassign", "--skip-child-origin-uplift"])

    cli.main()

    assert calls["cfg"].apply_child_origin_uplift is False


@pytest.mark.parametrize("command", ["run", "reassign", "matrices"])
def test_main_can_export_adjusted_csv(tmp_path: Path, monkeypatch, command):
    calls = []
    adjusted = Path("data/processed/reassign/trips_adjusted_step7.parquet")
    csv_dir = Path("outputs_step7/trips_adjusted_csv")

    def fake_export(source, output_dir):
        calls.append((source, output_dir))
        return [output_dir / "part-00000.csv"]

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_reassign", lambda *a, **kw: adjusted)
    monkeypatch.setattr(cli, "run_matrices", lambda *a, **kw: None)
    monkeypatch.setattr(cli, "export_adjusted_trip_csv", fake_export)
    monkeypatch.setattr(
        "sys.argv",
        [
            "uk-travel-pipeline", command,
            "--adjusted-parquet", str(adjusted),
            "--adjusted-csv-dir", str(csv_dir),
            "--skip-purpose-estimation",
        ],
    )

    cli.main()

    assert calls == [(adjusted, csv_dir)]


def test_main_passes_nts0502_purpose_calibration(tmp_path: Path, monkeypatch):
    calls = {}
    controls = tmp_path / "nts0502_controls.csv"

    def fake_run_reassign(cfg, legacy_output_root=None):
        calls["cfg"] = cfg
        return cfg.adjusted_parquet

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_reassign", fake_run_reassign)
    monkeypatch.setattr(
        "sys.argv",
        [
            "uk-travel-pipeline",
            "reassign",
            "--purpose-calibration",
            "nts0502",
            "--nts0502-period-purpose-csv",
            str(controls),
        ],
    )

    cli.main()

    assert calls["cfg"].purpose_calibration == "nts0502"
    assert calls["cfg"].nts0502_period_purpose_csv == controls


def test_matrices_split_road_mode_uses_split_mode_defaults(tmp_path: Path, monkeypatch):
    calls = {}

    def fake_run_matrices(cfg, legacy_output_root=None):
        calls["matrix_cfg"] = cfg
        return cfg.outputs_root

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_matrices", fake_run_matrices)
    monkeypatch.setattr("sys.argv", ["uk-travel-pipeline", "matrices", "--split-road-mode"])

    cli.main()

    assert calls["matrix_cfg"].modes == (
        "CYCLE",
        "PRIVATE_CAR",
        "MOTORCYCLE",
        "BUS",
        "RAIL",
        "WALKING",
        "SUBWAY",
    )


def test_main_runs_local_authority_use_case(tmp_path: Path, monkeypatch):
    calls = {}

    def fake_run_local_authority_use_case(cfg):
        calls["cfg"] = cfg

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_local_authority_use_case", fake_run_local_authority_use_case)
    monkeypatch.setattr(
        "sys.argv",
        [
            "uk-travel-pipeline",
            "local-authority",
            "--output-dir",
            "custom/local_authority",
            "--top-n",
            "7",
        ],
    )

    cli.main()

    assert calls["cfg"].output_dir == Path("custom/local_authority")
    assert calls["cfg"].top_n == 7


def test_main_runs_local_authority_visualisations(tmp_path: Path, monkeypatch):
    calls = {}

    def fake_run_local_authority_visualisations(cfg):
        calls["cfg"] = cfg

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli, "run_local_authority_visualisations", fake_run_local_authority_visualisations)
    monkeypatch.setattr(
        "sys.argv",
        [
            "uk-travel-pipeline",
            "local-authority-visualizations",
            "--input-dir",
            "custom/local_authority",
            "--output-dir",
            "custom/visuals",
            "--top-n",
            "6",
        ],
    )

    cli.main()

    assert calls["cfg"].input_dir == Path("custom/local_authority")
    assert calls["cfg"].output_dir == Path("custom/visuals")
    assert calls["cfg"].top_n == 6
