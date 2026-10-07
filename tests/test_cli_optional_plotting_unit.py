import subprocess
import sys
from pathlib import Path
from textwrap import dedent


def test_cli_import_and_parser_work_without_matplotlib():
    source_dir = Path(__file__).resolve().parents[1] / "src"
    script = dedent(
        """
        import importlib.abc
        import sys

        class BlockMatplotlib(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "matplotlib" or fullname.startswith("matplotlib."):
                    raise ModuleNotFoundError("Matplotlib is unavailable", name=fullname)

        sys.meta_path.insert(0, BlockMatplotlib())
        sys.path.insert(0, sys.argv[1])
        from uk_travel_pipeline import cli

        parser = cli.build_parser()
        args = parser.parse_args([
            "run", "--split-road-mode", "--skip-mode-time-constraint",
            "--skip-purpose-estimation", "--adjusted-csv-dir", "outputs_step7/csv",
        ])
        assert args.command == "run"
        assert args.skip_purpose_estimation
        assert "uk_travel_pipeline.local_authority_visuals" not in sys.modules
        assert "matplotlib" not in sys.modules

        visual_args = parser.parse_args(["local-authority-visuals"])
        assert visual_args.input_dir == cli.LocalAuthorityVisualConfig().input_dir

        try:
            cli.run_local_authority_visualisations(cli.LocalAuthorityVisualConfig())
        except ModuleNotFoundError as error:
            assert error.name == "matplotlib"
        else:
            raise AssertionError("The plotting command must load its plotting dependency")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(source_dir)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
