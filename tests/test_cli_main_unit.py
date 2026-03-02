from pathlib import Path

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
