"""Figure 1: consistency of the calibrated output with its survey controls.

(a) Mode shares in England before and after calibration, against the NTS target, summed over
    the region-band cells that have one. "After" is the stage at which the typical-week product
    is released: after calibration and the road split, before the mode-time refinement. The
    target weights each cell's NTS shares by that cell's uncalibrated volume, which is the mix
    calibration aims at given where volume lies.
(b) National weekday AM-peak purpose shares before raking (the local prior) and after,
    against the NTS0502 target.

Panel (a) is computed from share_check.csv and adjustment_factors.csv: the mode-time step
rescales only volume_adj, so the pre-refinement volume is the all-age base times the cell's
calibration factor. That reconstruction is checked against mode_time_share_check.csv. Panel (b) needs national sums
over the 51 GB purpose parquet, so they are cached in figures/nts0502_am_raking_summary.csv;
pass --recompute to rebuild the cache, which is checked against the pipeline's control file.

Usage, from paper/:  python3 scripts/make_validation_figure.py [--recompute]
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

PAPER = Path(__file__).resolve().parent.parent
ROOT = PAPER.parent
SHARE_CHECK = ROOT / "outputs/reassign/share_check.csv"
FACTORS = ROOT / "outputs/reassign/adjustment_factors.csv"
MODE_TIME_CHECK = ROOT / "outputs/reassign/mode_time_share_check.csv"
PURPOSE_PARQUET = ROOT / "data/processed/reassign/trips_adjusted_by_purpose.parquet"
CONTROL_CHECK = ROOT / "outputs/reassign/nts0502_purpose_control_check.csv"
RAKING_SUMMARY = PAPER / "figures/nts0502_am_raking_summary.csv"
OUT_A = PAPER / "figures/fig1a_mode_shares.pdf"
OUT_B = PAPER / "figures/fig1b_am_purpose_raking.pdf"
PERIOD = "weekday_AM"
FIGSIZE = (6.0, 2.7)  # printed at 0.92\textwidth, so roughly 1:1

MODES = {"WALKING": "Walking", "CYCLE": "Cycle", "PRIVATE_CAR": "Private car", "MOTORCYCLE": "Motorcycle",
         "BUS": "Bus", "RAIL": "Rail", "SUBWAY": "Subway"}
ROAD = {"CYCLE", "PRIVATE_CAR", "MOTORCYCLE", "BUS"}
PURPOSES = {"commuting": "Commute", "business": "Business", "education": "Education", "shopping": "Shopping",
            "personal_business": "Personal\nbusiness", "social_vfr": "Social/VFR", "holiday": "Holiday"}


# --- shared drawing ----------------------------------------------------------
def draw_before_after(df: pd.DataFrame, names: dict[str, str], stages: tuple[str, str, str],
                      ylabel: str, out: Path) -> None:
    """Grouped before/after bars with a black target line across each group."""
    before, after, target = stages
    long = (df[["before", "after"]].rename(index=names).rename(columns={"before": before, "after": after})
            .rename_axis("group").reset_index().melt(id_vars="group", var_name="Stage", value_name="value"))
    fig, ax = plt.subplots(figsize=FIGSIZE)
    sns.barplot(data=long, x="group", y="value", hue="Stage", ax=ax)
    for i, t in enumerate(df["target"]):
        ax.hlines(t, i - 0.42, i + 0.42, color="black", lw=1.4, zorder=5)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles + [Line2D([], [], color="black", lw=1.4)], labels + [target], title=None, frameon=False)
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


# --- panel (a) ---------------------------------------------------------------
def calibrated_cells() -> pd.DataFrame:
    """Region-band-mode cells with volume after calibration but before the mode-time refinement."""
    c = pd.read_csv(SHARE_CHECK)
    f = pd.read_csv(FACTORS).rename(columns={"bt_mode": "broad"})
    c["broad"] = np.where(c["bt_mode"].isin(ROAD), "ROAD", c["bt_mode"])
    c = c.merge(f, on=["origin_region", "distance_band", "broad"], how="left", validate="many_to_one")
    assert c["factor"].notna().all(), "every share-check cell should have a calibration factor"
    c["volume_cal"] = c["volume_all_age_base"] * c["factor"]

    # The mode-time check records the same pre-refinement volumes by origin; they must agree.
    eng = c[c["origin_region"] != "Wales"]
    rebuilt = eng.groupby(eng["bt_mode"].replace({"MOTORCYCLE": "PRIVATE_CAR"}))["volume_cal"].sum()
    mt = pd.read_csv(MODE_TIME_CHECK, usecols=["origin_msoa", "mode_time_group", "volume_adj"])
    logged = mt[mt["origin_msoa"].str.startswith("E")].groupby("mode_time_group")["volume_adj"].sum()
    worst = (rebuilt / logged.reindex(rebuilt.index) - 1).abs().max()
    if worst > 1e-9:
        raise SystemExit(f"reconstructed pre-refinement volumes differ from the mode-time check by {worst:.2e}")

    c = c.dropna(subset=["nts_share"])
    assert len(c) == 427, f"expected 427 targeted cells, found {len(c)}"
    cell = ["origin_region", "distance_band"]
    c["share_cal"] = c["volume_cal"] / c.groupby(cell)["volume_cal"].transform("sum")
    c["cell_base"] = c.groupby(cell)["volume_all_age_base"].transform("sum")
    c["cell_cal"] = c.groupby(cell)["volume_cal"].transform("sum")
    c["gap_before"] = 100 * (c["base_share_before"] - c["nts_share"]).abs()
    c["gap_after"] = 100 * (c["share_cal"] - c["nts_share"]).abs()
    return c


def mode_shares(c: pd.DataFrame) -> pd.DataFrame:
    by_mode = c.groupby("bt_mode").agg(before=("volume_all_age_base", "sum"), after=("volume_cal", "sum"))
    by_mode["target"] = (c["nts_share"] * c["cell_base"]).groupby(c["bt_mode"]).sum()
    return (100 * by_mode / by_mode.sum()).reindex(list(MODES))


def report_mode_check(c: pd.DataFrame, shares: pd.DataFrame) -> None:
    """Print every number the Validation text quotes for panel (a)."""
    w_before = np.average(c["gap_before"], weights=c["cell_base"])
    w_after = np.average(c["gap_after"], weights=c["cell_cal"])
    per_mode = c.groupby("bt_mode")[["gap_before", "gap_after"]].mean()
    road = shares.loc[[m for m in MODES if m in ROAD]].sum()
    print(f"panel (a), 427 cells: mean gap {c['gap_before'].mean():.2f} -> {c['gap_after'].mean():.3f} pp; "
          f"cell-volume-weighted {w_before:.2f} -> {w_after:.3f} pp; cells still > 0.5 pp: {(c['gap_after'] > 0.5).sum()}")
    for m in ("PRIVATE_CAR", "WALKING"):
        print(f"  {MODES[m]:12s} gap {per_mode.loc[m, 'gap_before']:.2f} -> {per_mode.loc[m, 'gap_after']:.3f} pp")
    print(f"  England road / walking: before {road['before']:.1f} / {shares.loc['WALKING', 'before']:.1f}, "
          f"after {road['after']:.1f} / {shares.loc['WALKING', 'after']:.1f}, "
          f"target {road['target']:.1f} / {shares.loc['WALKING', 'target']:.1f}")


# --- panel (b) ---------------------------------------------------------------
def compute_raking_shares() -> pd.DataFrame:
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    data = ds.dataset(PURPOSE_PARQUET, format="parquet")
    cols = ["purpose_control_group", "volume_adj", "local_prior_share", "volume_adj_purpose"]
    prior, raked = defaultdict(float), defaultdict(float)
    for batch in data.to_batches(columns=cols, filter=pc.field("purpose_period_key") == PERIOD, batch_size=1_000_000):
        if batch.num_rows == 0:
            continue
        keys, inv = np.unique(np.array(batch.column("purpose_control_group").to_pylist(), dtype=object), return_inverse=True)
        w_prior = pc.multiply(batch.column("volume_adj"), batch.column("local_prior_share")).to_numpy(zero_copy_only=False)
        w_raked = batch.column("volume_adj_purpose").to_numpy(zero_copy_only=False)
        for k, sp, sr in zip(keys, np.bincount(inv, w_prior), np.bincount(inv, w_raked)):
            prior[k] += sp
            raked[k] += sr
    tp, tr = sum(prior.values()), sum(raked.values())
    return pd.DataFrame({"control_group": sorted(raked),
                         "before_raking_pct": [100 * prior[k] / tp for k in sorted(raked)],
                         "after_raking_pct": [100 * raked[k] / tr for k in sorted(raked)],
                         "am_total_volume": tr})


def raking_shares(recompute: bool) -> pd.DataFrame:
    if recompute or not RAKING_SUMMARY.exists():
        shares = compute_raking_shares()
        check = pd.read_csv(CONTROL_CHECK)
        check = check[check["purpose_period_key"] == PERIOD][
            ["control_group", "target_control_share", "actual_control_share", "period_total_trips"]]
        m = shares.merge(check, on="control_group", how="inner", validate="one_to_one")
        # The parquet and the control check must describe the same raked allocation.
        share_gap = (m["after_raking_pct"] - 100 * m["actual_control_share"]).abs().max()
        total_gap = abs(m["am_total_volume"].iloc[0] / m["period_total_trips"].iloc[0] - 1)
        if share_gap > 1e-6 or total_gap > 1e-9:
            raise SystemExit(f"purpose parquet does not match the control check "
                             f"(share gap {share_gap:.2e} pp, total gap {total_gap:.2e})")
        m["target_pct"] = 100 * m["target_control_share"]
        m[["control_group", "before_raking_pct", "target_pct", "after_raking_pct"]].to_csv(
            RAKING_SUMMARY, index=False, float_format="%.8f")
    return pd.read_csv(RAKING_SUMMARY).set_index("control_group").reindex(list(PURPOSES))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--recompute", action="store_true",
                   help="Recompute panel (b) shares from the purpose parquet (about 5 minutes).")
    args = p.parse_args()
    OUT_A.parent.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid", context="paper")
    cells = calibrated_cells()
    modes = mode_shares(cells)
    draw_before_after(modes, MODES, ("Before calibration", "After calibration", "NTS target"),
                      "Mode share,\nEngland (%)", OUT_A)
    purposes = raking_shares(args.recompute).rename(
        columns={"before_raking_pct": "before", "after_raking_pct": "after", "target_pct": "target"})
    draw_before_after(purposes, PURPOSES, ("Before raking (local prior)", "After raking", "NTS0502 target"),
                      "Share of weekday\nAM-peak volume (%)", OUT_B)
    print(f"wrote {OUT_A.relative_to(PAPER)} and {OUT_B.relative_to(PAPER)}")
    report_mode_check(cells, modes)

if __name__ == "__main__":
    main()
