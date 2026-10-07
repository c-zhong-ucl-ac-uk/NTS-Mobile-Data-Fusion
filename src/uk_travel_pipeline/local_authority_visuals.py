from __future__ import annotations

from pathlib import Path

import os

os.environ.setdefault("MPLCONFIGDIR", "/tmp/uk_travel_pipeline_matplotlib")
os.environ.setdefault("XDG_CACHE_HOME", "/tmp/uk_travel_pipeline_cache")

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, Patch

from .config import LocalAuthorityVisualConfig
from .io import assert_files_exist, ensure_dir, write_csv
from .local_authority import TRIPS_COL


FIGURE_DPI = 180
PALETTE = {
    "blue": "#2f6690",
    "teal": "#2a9d8f",
    "green": "#5f8d4e",
    "gold": "#d9a441",
    "coral": "#d46a6a",
    "indigo": "#5b5f97",
    "grey": "#6c757d",
    "light": "#eef2f3",
    "dark": "#24323f",
}
MODE_COLOURS = {
    "Private car": "#2f6690",
    "Bus": "#2a9d8f",
    "Walking": "#d9a441",
    "Cycle": "#5f8d4e",
    "Motorcycle": "#d46a6a",
    "Rail": "#5b5f97",
    "Subway": "#8a6f91",
    "ROAD": "#6c757d",
}
PURPOSE_COLOURS = {
    "Commuting": "#2f6690",
    "Employer Business": "#5b5f97",
    "Education": "#5f8d4e",
    "Shopping": "#d9a441",
    "Personal Business": "#2a9d8f",
    "Social/Leisure": "#d46a6a",
    "Visiting Friends/Relatives": "#8a6f91",
    "Holiday/Day Trip": "#6c757d",
}
PERIOD_LABELS = {
    "weekday_AM": "Weekday AM",
    "weekday_PM": "Weekday PM",
    "weekday_off_peak": "Weekday off-peak",
    "weekend": "Weekend",
}


def _set_style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#c9d1d3",
            "axes.labelcolor": PALETTE["dark"],
            "axes.titlecolor": PALETTE["dark"],
            "xtick.color": PALETTE["dark"],
            "ytick.color": PALETTE["dark"],
            "font.size": 10,
            "axes.titlesize": 15,
            "axes.titleweight": "bold",
            "axes.labelsize": 10,
            "legend.frameon": False,
            "savefig.bbox": "tight",
        }
    )


def _trips_thousands(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0) / 1000.0


def _percent(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0) * 100.0


def _clean_label(value: object) -> str:
    return str(value).replace("Dunstable core", "Dunstable").replace("weekday_", "")


def _save(fig: plt.Figure, path: Path) -> Path:
    ensure_dir(path.parent)
    fig.savefig(path, dpi=FIGURE_DPI)
    plt.close(fig)
    return path


def _read(input_dir: Path, relative_path: str) -> pd.DataFrame:
    return pd.read_csv(input_dir / relative_path)


def _bar_label(ax, fmt: str = "{:.0f}") -> None:
    for container in ax.containers:
        labels = []
        for value in container.datavalues:
            labels.append(fmt.format(value) if value > 0 else "")
        ax.bar_label(container, labels=labels, padding=3, fontsize=8, color=PALETTE["dark"])


def _figure_manifest_row(path: Path, title: str, description: str) -> dict[str, str]:
    return {
        "title": title,
        "description": description,
        "path": str(path),
    }


def _plot_corridor_demand(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_dunstable/bidirectional_demand.csv")
    df = df[df["direction"] != "Combined"].copy()
    df["trips_k"] = _trips_thousands(df[TRIPS_COL])
    df["direction"] = df["direction"].map(_clean_label)
    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    colours = [PALETTE["blue"], PALETTE["teal"]]
    ax.barh(df["direction"], df["trips_k"], color=colours[: len(df)], height=0.55)
    ax.set_title("Luton-Dunstable Corridor Demand")
    ax.set_xlabel("Typical-week trips, thousands")
    ax.grid(axis="x", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right", "left"]].set_visible(False)
    for i, value in enumerate(df["trips_k"]):
        ax.text(value + 2, i, f"{value:,.0f}k", va="center", fontsize=9)
    path = _save(fig, output_dir / "01_corridor_bidirectional_demand.png")
    return _figure_manifest_row(path, "Corridor Demand", "Directional typical-week demand between Luton and Dunstable core.")


def _plot_corridor_mode_share(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_dunstable/mode_shares.csv")
    df = df[df["direction"] == "Combined"].copy()
    df = df.sort_values("share", ascending=True)
    df["share_pct"] = _percent(df["share"])
    colours = [MODE_COLOURS.get(label, PALETTE["grey"]) for label in df["mode_label"]]
    fig, ax = plt.subplots(figsize=(8.5, 5.4))
    ax.barh(df["mode_label"], df["share_pct"], color=colours, height=0.62)
    ax.set_title("Luton-Dunstable Mode Share")
    ax.set_xlabel("Share of corridor trips (%)")
    ax.grid(axis="x", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right", "left"]].set_visible(False)
    for i, value in enumerate(df["share_pct"]):
        ax.text(value + 0.8, i, f"{value:.1f}%", va="center", fontsize=9)
    path = _save(fig, output_dir / "02_corridor_mode_share.png")
    return _figure_manifest_row(path, "Corridor Mode Share", "Combined Luton-Dunstable mode split.")


def _plot_corridor_purpose_share(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_dunstable/purpose_shares.csv")
    df = df[df["direction"] == "Combined"].copy()
    df["trips_k"] = _trips_thousands(df[TRIPS_COL])
    colours = [PURPOSE_COLOURS.get(label, PALETTE["grey"]) for label in df["purpose_desc"]]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.bar(df["purpose_desc"], df["trips_k"], color=colours, width=0.72)
    ax.set_title("Luton-Dunstable Demand by Purpose")
    ax.set_ylabel("Typical-week trips, thousands")
    ax.grid(axis="y", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="x", rotation=35)
    _bar_label(ax, "{:.0f}k")
    path = _save(fig, output_dir / "03_corridor_purpose_profile.png")
    return _figure_manifest_row(path, "Corridor Purpose Profile", "Purpose composition of combined corridor demand.")


def _plot_corridor_time_profile(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_dunstable/time_period_profile.csv")
    df = df[df["direction"] == "Combined"].copy()
    df["period_label"] = df["period_profile"].map(PERIOD_LABELS).fillna(df["period_profile"])
    df["trips_k"] = _trips_thousands(df[TRIPS_COL])
    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    ax.plot(df["period_label"], df["trips_k"], color=PALETTE["blue"], marker="o", linewidth=2.5)
    ax.fill_between(df["period_label"], df["trips_k"], color=PALETTE["blue"], alpha=0.12)
    ax.set_title("Luton-Dunstable Time-Period Profile")
    ax.set_ylabel("Typical-week trips, thousands")
    ax.grid(axis="y", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    for x, y in zip(df["period_label"], df["trips_k"], strict=False):
        ax.text(x, y + 4, f"{y:.0f}k", ha="center", fontsize=8)
    path = _save(fig, output_dir / "04_corridor_time_profile.png")
    return _figure_manifest_row(path, "Corridor Time Profile", "Demand split across AM, PM, off-peak, and weekend periods.")


def _plot_corridor_top_od(input_dir: Path, output_dir: Path, top_n: int) -> dict[str, str]:
    df = _read(input_dir, "luton_dunstable/top_od_pairs.csv")
    df = df.sort_values(TRIPS_COL, ascending=False).head(top_n).copy()
    df["pair"] = df["origin_msoa_name"] + " -> " + df["destination_msoa_name"]
    df["trips_k"] = _trips_thousands(df[TRIPS_COL])
    df = df.sort_values("trips_k")
    fig, ax = plt.subplots(figsize=(10, 6.8))
    ax.barh(df["pair"], df["trips_k"], color=PALETTE["teal"], height=0.58)
    ax.set_title(f"Top {top_n} Luton-Dunstable OD Pairs")
    ax.set_xlabel("Typical-week trips, thousands")
    ax.grid(axis="x", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right", "left"]].set_visible(False)
    path = _save(fig, output_dir / "05_corridor_top_od_pairs.png")
    return _figure_manifest_row(path, "Corridor Top OD Pairs", "Highest-volume MSOA-to-MSOA flows on the corridor.")


def _plot_airport_movement(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_airport/movement_summary.csv")
    pivot = df.pivot(index="area_scope", columns="movement", values=TRIPS_COL).fillna(0.0) / 1000.0
    pivot = pivot[[col for col in ["Inbound", "Outbound", "Internal"] if col in pivot.columns]]
    fig, ax = plt.subplots(figsize=(9, 5.2))
    pivot.plot(
        kind="bar",
        ax=ax,
        color=[PALETTE["blue"], PALETTE["teal"], PALETTE["gold"]][: len(pivot.columns)],
        width=0.72,
    )
    ax.set_title("Luton Airport Area Surface-Access Movements")
    ax.set_xlabel("")
    ax.set_ylabel("Typical-week trips, thousands")
    ax.grid(axis="y", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="x", rotation=0)
    path = _save(fig, output_dir / "06_airport_movement_summary.png")
    return _figure_manifest_row(path, "Airport Movement Summary", "Inbound, outbound, and internal trips for airport core and wider areas.")


def _stacked_share(
    ax,
    df: pd.DataFrame,
    index_col: str,
    category_col: str,
    value_col: str,
    colours: dict[str, str],
) -> None:
    pivot = df.pivot_table(index=index_col, columns=category_col, values=value_col, aggfunc="sum").fillna(0.0)
    totals = pivot.sum(axis=1).replace(0, np.nan)
    pivot = pivot.div(totals, axis=0).fillna(0.0) * 100.0
    left = np.zeros(len(pivot))
    y = np.arange(len(pivot))
    for col in pivot.columns:
        values = pivot[col].to_numpy()
        ax.barh(y, values, left=left, label=col, color=colours.get(col, PALETTE["grey"]), height=0.65)
        left += values
    ax.set_yticks(y)
    ax.set_yticklabels(pivot.index)
    ax.set_xlim(0, 100)
    ax.grid(axis="x", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right", "left"]].set_visible(False)


def _plot_airport_catchment(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_airport/catchment_summary.csv")
    df = df[df["movement"].isin(["Inbound", "Outbound"])].copy()
    df["segment"] = df["area_scope"] + " " + df["movement"]
    catchment_colours = {
        "Rest of Luton": PALETTE["blue"],
        "Dunstable / wider Central Bedfordshire": PALETTE["teal"],
        "Rest of EEH": PALETTE["gold"],
        "Rest of England and Wales": PALETTE["coral"],
    }
    fig, ax = plt.subplots(figsize=(10.5, 5.6))
    _stacked_share(ax, df, "segment", "catchment", TRIPS_COL, catchment_colours)
    ax.set_title("Luton Airport Catchment Composition")
    ax.set_xlabel("Share of inbound/outbound trips (%)")
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.28), ncol=2)
    path = _save(fig, output_dir / "07_airport_catchment_share.png")
    return _figure_manifest_row(path, "Airport Catchment Share", "Catchment mix for airport-area inbound and outbound movements.")


def _plot_airport_mode_share(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_airport/mode_shares.csv")
    df = df[df["area_scope"] == "Airport core"].copy()
    df["segment"] = df["movement"]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    _stacked_share(ax, df, "segment", "mode_label", TRIPS_COL, MODE_COLOURS)
    ax.set_title("Airport Core Mode Share by Movement")
    ax.set_xlabel("Share of trips (%)")
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.30), ncol=4)
    path = _save(fig, output_dir / "08_airport_core_mode_share.png")
    return _figure_manifest_row(path, "Airport Core Mode Share", "Mode split for inbound, outbound, and internal airport-core movements.")


def _plot_airport_worker_access(input_dir: Path, output_dir: Path) -> dict[str, str]:
    df = _read(input_dir, "luton_airport/worker_access_summary.csv")
    df = df[(df["area_scope"] == "Airport core") & (df["movement"].isin(["Inbound", "Outbound"]))].copy()
    grouped = df.groupby(["movement", "purpose_desc"], as_index=False)[TRIPS_COL].sum()
    grouped["trips_k"] = _trips_thousands(grouped[TRIPS_COL])
    grouped["segment"] = grouped["movement"] + " " + grouped["purpose_desc"]
    fig, ax = plt.subplots(figsize=(9.5, 5.2))
    colours = [PURPOSE_COLOURS.get(p, PALETTE["grey"]) for p in grouped["purpose_desc"]]
    ax.barh(grouped["segment"], grouped["trips_k"], color=colours, height=0.58)
    ax.set_title("Airport Core Worker-Access Proxy")
    ax.set_xlabel("Typical-week trips, thousands")
    ax.grid(axis="x", color="#e5e8e8", linewidth=0.8)
    ax.spines[["top", "right", "left"]].set_visible(False)
    path = _save(fig, output_dir / "09_airport_worker_access_proxy.png")
    return _figure_manifest_row(path, "Airport Worker-Access Proxy", "Commuting and employer-business demand around the airport core.")


def _plot_airport_top_origins_destinations(input_dir: Path, output_dir: Path, top_n: int) -> dict[str, str]:
    origins = _read(input_dir, "luton_airport/top_20_origins_to_airport.csv")
    destinations = _read(input_dir, "luton_airport/top_20_destinations_from_airport.csv")
    origins = origins[origins["area_scope"] == "Airport core"].head(top_n).copy()
    destinations = destinations[destinations["area_scope"] == "Airport core"].head(top_n).copy()
    origins["trips_k"] = _trips_thousands(origins[TRIPS_COL])
    destinations["trips_k"] = _trips_thousands(destinations[TRIPS_COL])

    fig, axes = plt.subplots(1, 2, figsize=(13, 6.5), sharex=False)
    for ax, df, label_col, title, colour in [
        (axes[0], origins.sort_values("trips_k"), "origin_msoa_name", "Origins to Airport Core", PALETTE["blue"]),
        (
            axes[1],
            destinations.sort_values("trips_k"),
            "destination_msoa_name",
            "Destinations from Airport Core",
            PALETTE["teal"],
        ),
    ]:
        ax.barh(df[label_col], df["trips_k"], color=colour, height=0.58)
        ax.set_title(title)
        ax.set_xlabel("Typical-week trips, thousands")
        ax.grid(axis="x", color="#e5e8e8", linewidth=0.8)
        ax.spines[["top", "right", "left"]].set_visible(False)
    path = _save(fig, output_dir / "10_airport_top_origins_destinations.png")
    return _figure_manifest_row(path, "Airport Top Origins And Destinations", "Highest-volume MSOA origins and destinations for the airport core.")


def _read_msoa_boundaries(msoa_geojson: Path, definitions: pd.DataFrame) -> gpd.GeoDataFrame:
    definitions = definitions.drop_duplicates("MSOA21CD").copy()
    codes = definitions["MSOA21CD"].dropna().astype(str).unique().tolist()
    gdf = gpd.read_file(msoa_geojson)
    gdf["MSOA21CD"] = gdf["MSOA21CD"].astype(str)
    gdf = gdf[gdf["MSOA21CD"].isin(codes)].copy()
    return gdf.merge(definitions[["MSOA21CD", "area_key", "area_label"]], on="MSOA21CD", how="left")


def _plot_flow_map(
    input_dir: Path,
    output_dir: Path,
    msoa_geojson: Path,
    flow_relative_path: str,
    title: str,
    output_name: str,
    area_keys: tuple[str, ...],
    flow_area_scope: str | None = None,
) -> dict[str, str]:
    definitions = _read(input_dir, "area_msoa_definitions.csv")
    definitions = definitions[definitions["area_key"].isin(area_keys)].copy()
    definitions["_area_order"] = definitions["area_key"].map({key: i for i, key in enumerate(area_keys)})
    definitions = definitions.sort_values("_area_order").drop_duplicates("MSOA21CD")
    boundaries = _read_msoa_boundaries(msoa_geojson, definitions)
    flows = gpd.read_file(input_dir / flow_relative_path)
    if flow_area_scope is not None:
        flows = flows[flows["area_scope"].eq(flow_area_scope)].copy()
    group_col = "direction" if "direction" in flows.columns else "movement"
    top = (
        flows.sort_values(TRIPS_COL, ascending=False)
        .groupby(group_col, sort=False, group_keys=False)
        .head(8)
        .copy()
    )

    min_x, min_y, max_x, max_y = top.total_bounds
    x_pad = max((max_x - min_x) * 0.10, 0.008)
    y_pad = max((max_y - min_y) * 0.10, 0.008)
    plot_bounds = (min_x - x_pad, min_y - y_pad, max_x + x_pad, max_y + y_pad)
    context = gpd.read_file(msoa_geojson, bbox=plot_bounds)

    is_wide = (max_x - min_x) > 1.25 * (max_y - min_y)
    fig, ax = plt.subplots(figsize=(10, 6.3) if is_wide else (8.4, 7.4))
    context.plot(ax=ax, facecolor="#f7f8f8", edgecolor="#b8c1c6", linewidth=0.6, zorder=0)

    area_handles = []
    area_colours = {
        "luton_lad": "#d6e8f5",
        "dunstable_core": "#cfece7",
        "dunstable_wider": "#f4e4b8",
        "airport_core": "#f2cccc",
        "airport_wider": "#d8d9ee",
    }
    for area_key, group in boundaries.groupby("area_key"):
        group.plot(
            ax=ax,
            facecolor=area_colours.get(area_key, "#f4f6f7"),
            edgecolor="#4e5962",
            linewidth=0.9,
            alpha=0.72,
            label=str(group["area_label"].iloc[0]),
            zorder=1,
        )
        area_label = str(group["area_label"].iloc[0])
        area_handles.append(Patch(facecolor=area_colours.get(area_key, "#f4f6f7"), edgecolor="#4e5962", label=area_label))

    category_colours = {
        "Luton to Dunstable core": PALETTE["blue"],
        "Dunstable core to Luton": PALETTE["coral"],
        "Inbound": PALETTE["teal"],
        "Outbound": PALETTE["coral"],
    }
    max_trips = float(top[TRIPS_COL].max())
    for row in top.itertuples(index=False):
        coords = list(row.geometry.coords)
        width = 0.9 + 4.6 * np.sqrt(float(getattr(row, TRIPS_COL)) / max_trips)
        arrow = FancyArrowPatch(
            coords[0],
            coords[-1],
            arrowstyle="-|>",
            mutation_scale=7.5 + width,
            connectionstyle="arc3,rad=0.035",
            linewidth=width,
            color=category_colours.get(str(getattr(row, group_col)), PALETTE["coral"]),
            alpha=0.66,
            shrinkA=1.5,
            shrinkB=1.5,
            zorder=4,
        )
        ax.add_patch(arrow)

    endpoint_codes = pd.unique(pd.concat([top["origin_msoa"], top["destination_msoa"]], ignore_index=True))
    endpoint_points = pd.concat(
        [
            top[["origin_msoa", "origin_msoa_name", "origin_lon", "origin_lat"]].rename(
                columns={"origin_msoa": "MSOA21CD", "origin_msoa_name": "name", "origin_lon": "lon", "origin_lat": "lat"}
            ),
            top[["destination_msoa", "destination_msoa_name", "destination_lon", "destination_lat"]].rename(
                columns={
                    "destination_msoa": "MSOA21CD",
                    "destination_msoa_name": "name",
                    "destination_lon": "lon",
                    "destination_lat": "lat",
                }
            ),
        ],
        ignore_index=True,
    ).drop_duplicates("MSOA21CD")
    endpoint_points = endpoint_points[endpoint_points["MSOA21CD"].isin(endpoint_codes)]
    ax.scatter(endpoint_points["lon"], endpoint_points["lat"], s=13, color=PALETTE["dark"], zorder=5)
    if flow_area_scope is not None:
        external_points = endpoint_points[~endpoint_points["MSOA21CD"].isin(definitions["MSOA21CD"])].copy()
        external_totals = pd.concat(
            [
                top[["origin_msoa", TRIPS_COL]].rename(columns={"origin_msoa": "MSOA21CD"}),
                top[["destination_msoa", TRIPS_COL]].rename(columns={"destination_msoa": "MSOA21CD"}),
            ],
            ignore_index=True,
        ).groupby("MSOA21CD", as_index=False)[TRIPS_COL].sum()
        external_points = external_points.merge(external_totals, on="MSOA21CD", how="left").nlargest(3, TRIPS_COL)
        for point in external_points.itertuples(index=False):
            ax.annotate(
                str(point.name),
                (point.lon, point.lat),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=7,
                color=PALETTE["dark"],
                bbox={"boxstyle": "round,pad=0.15", "facecolor": "white", "edgecolor": "none", "alpha": 0.82},
                zorder=6,
            )

    direction_handles = [
        Line2D([0], [0], color=category_colours.get(str(category), PALETTE["coral"]), linewidth=3, label=str(category))
        for category in top[group_col].drop_duplicates()
    ]
    width_values = sorted({max(1000, int(round(max_trips / 2 / 1000) * 1000)), int(round(max_trips / 1000) * 1000)})
    width_handles = [
        Line2D(
            [0],
            [0],
            color=PALETTE["grey"],
            linewidth=0.9 + 4.6 * np.sqrt(value / max_trips),
            alpha=0.66,
            label=f"{value / 1000:.0f}k trips/week",
        )
        for value in width_values
    ]
    boundaries.boundary.plot(ax=ax, color=PALETTE["dark"], linewidth=0.9, zorder=2)
    ax.set_title(title)
    ax.set_axis_off()
    ax.set_xlim(plot_bounds[0], plot_bounds[2])
    ax.set_ylim(plot_bounds[1], plot_bounds[3])
    ax.legend(
        handles=area_handles + direction_handles + width_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.015),
        fontsize=8,
        ncol=3,
        title="Selected areas, direction and flow volume",
        title_fontsize=8,
    )
    fig.subplots_adjust(bottom=0.20)
    path = _save(fig, output_dir / output_name)
    return _figure_manifest_row(
        path,
        title,
        "Top eight OD cells in each direction; arrows show direction and line width shows representative-week trips.",
    )


def _write_index(output_dir: Path, manifest: pd.DataFrame) -> None:
    rows = []
    for item in manifest.itertuples(index=False):
        rel = Path(item.path).name
        rows.append(
            f"""
            <section>
              <h2>{item.title}</h2>
              <p>{item.description}</p>
              <img src="{rel}" alt="{item.title}">
            </section>
            """
        )
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Local Authority Case Study Visualisations</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: #24323f; background: #f7f9fa; }}
    header {{ padding: 28px 36px; background: #24323f; color: white; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 24px; }}
    section {{ margin: 0 0 28px; padding: 22px; background: white; border: 1px solid #d8e0e3; border-radius: 8px; }}
    h1, h2 {{ margin: 0 0 10px; }}
    p {{ margin: 0 0 16px; line-height: 1.45; }}
    img {{ width: 100%; height: auto; display: block; border: 1px solid #e1e7e9; }}
  </style>
</head>
<body>
  <header>
    <h1>Local Authority Case Study Visualisations</h1>
    <p>Luton-Dunstable and Luton Airport evidence pack.</p>
  </header>
  <main>
    {''.join(rows)}
  </main>
</body>
</html>
"""
    (output_dir / "index.html").write_text(html, encoding="utf-8")


def run_local_authority_visualisations(config: LocalAuthorityVisualConfig) -> None:
    required = [
        config.input_dir / "area_msoa_definitions.csv",
        config.input_dir / "luton_dunstable" / "bidirectional_demand.csv",
        config.input_dir / "luton_dunstable" / "mode_shares.csv",
        config.input_dir / "luton_dunstable" / "purpose_shares.csv",
        config.input_dir / "luton_dunstable" / "time_period_profile.csv",
        config.input_dir / "luton_dunstable" / "top_od_pairs.csv",
        config.input_dir / "luton_dunstable" / "corridor_flows_map.geojson",
        config.input_dir / "luton_airport" / "movement_summary.csv",
        config.input_dir / "luton_airport" / "catchment_summary.csv",
        config.input_dir / "luton_airport" / "mode_shares.csv",
        config.input_dir / "luton_airport" / "worker_access_summary.csv",
        config.input_dir / "luton_airport" / "top_20_origins_to_airport.csv",
        config.input_dir / "luton_airport" / "top_20_destinations_from_airport.csv",
        config.input_dir / "luton_airport" / "airport_flows_map.geojson",
        config.msoa_geojson,
    ]
    assert_files_exist(required)
    ensure_dir(config.output_dir)
    _set_style()

    manifest_rows = [
        _plot_corridor_demand(config.input_dir, config.output_dir),
        _plot_corridor_mode_share(config.input_dir, config.output_dir),
        _plot_corridor_purpose_share(config.input_dir, config.output_dir),
        _plot_corridor_time_profile(config.input_dir, config.output_dir),
        _plot_corridor_top_od(config.input_dir, config.output_dir, config.top_n),
        _plot_flow_map(
            config.input_dir,
            config.output_dir,
            config.msoa_geojson,
            "luton_dunstable/corridor_flows_map.geojson",
            "Top Directional OD Flows: Luton-Dunstable",
            "11_corridor_flow_map.png",
            ("luton_lad", "dunstable_core"),
            None,
        ),
        _plot_airport_movement(config.input_dir, config.output_dir),
        _plot_airport_catchment(config.input_dir, config.output_dir),
        _plot_airport_mode_share(config.input_dir, config.output_dir),
        _plot_airport_worker_access(config.input_dir, config.output_dir),
        _plot_airport_top_origins_destinations(config.input_dir, config.output_dir, config.top_n),
        _plot_flow_map(
            config.input_dir,
            config.output_dir,
            config.msoa_geojson,
            "luton_airport/airport_flows_map.geojson",
            "Top Directional OD Flows Touching Luton Airport Core",
            "12_airport_flow_map.png",
            ("airport_core", "airport_wider"),
            "Airport core",
        ),
    ]
    manifest = pd.DataFrame(manifest_rows)
    write_csv(manifest, config.output_dir / "manifest.csv")
    _write_index(config.output_dir, manifest)
