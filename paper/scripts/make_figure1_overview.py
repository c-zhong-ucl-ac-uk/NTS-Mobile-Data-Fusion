"""Figure 1: pipeline overview (data inputs -> calibration pipeline -> outputs).

Redraws the stakeholder-slide overview as a manuscript figure whose content
matches the paper: five input streams, five calibration steps, and the three
released output families. Writes a vector PDF and a 300-dpi PNG.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.font_manager as fm  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Circle, FancyArrowPatch, FancyBboxPatch  # noqa: E402

FIGURES_DIR = Path(__file__).resolve().parents[1] / "figures"

# Palette --------------------------------------------------------------------
INK = "#243244"          # primary text
SUB = "#5B6675"          # body text
BLUE_BG, BLUE_ED, BLUE_TX = "#EAF1FD", "#C6D8F4", "#1E4A8A"
ORANGE_BG, ORANGE_ED, ORANGE_TX = "#FEF3E6", "#F4D6B2", "#9A5312"
ORANGE_DISC = "#F5872E"
GREEN_BG, GREEN_ED, GREEN_TX = "#EAF6EF", "#C4E4CF", "#1C5B36"
CARD_ED = "#DCE4EE"

plt.rcParams["font.family"] = "sans-serif"
plt.rcParams["font.sans-serif"] = ["Helvetica", "Arial", "DejaVu Sans"]

W, H = 1320.0, 710.0


def panel(ax, x, y, w, h, bg, ed):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0,rounding_size=16",
        facecolor=bg, edgecolor=ed, linewidth=1.4, zorder=1))


def card(ax, x, y, w, h, ed=CARD_ED, bg="white", lw=1.2):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0,rounding_size=10",
        facecolor=bg, edgecolor=ed, linewidth=lw, zorder=2))


def heading(ax, x, y, text, color):
    ax.text(x, y, text, ha="center", va="center", color=color,
            fontsize=15, fontweight="bold", zorder=3)


def title_body(ax, x, ytop, title, body, wrap, tcolor=INK, tsize=11.5,
               bsize=9.3, xpad=16):
    ax.text(x + xpad, ytop, title, ha="left", va="top", color=tcolor,
            fontsize=tsize, fontweight="bold", linespacing=1.3, zorder=3)
    wrapped = "\n".join(textwrap.wrap(body, wrap))
    body_offset = 24 + 19 * title.count("\n")
    ax.text(x + xpad, ytop - body_offset, wrapped, ha="left", va="top",
            color=SUB, fontsize=bsize, linespacing=1.35, zorder=3)


def arrow(ax, x0, x1, y):
    ax.add_patch(FancyArrowPatch(
        (x0, y), (x1, y), arrowstyle="-|>", mutation_scale=26,
        color="#7C8AA9", linewidth=3.2, zorder=4))


def tag(ax, x, y, text, color):
    w = 18 + 7.2 * len(text)
    ax.add_patch(FancyBboxPatch(
        (x, y - 11), w, 22, boxstyle="round,pad=0,rounding_size=11",
        facecolor="white", edgecolor=color, linewidth=1.2, zorder=3))
    ax.text(x + w / 2, y, text, ha="center", va="center", color=color,
            fontsize=8.6, fontweight="bold", zorder=4)
    return w


def build():
    fig = plt.figure(figsize=(13.2, 7.1), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis("off")

    # Panel geometry
    lx, lw = 28, 336
    mx, mw = 418, 452
    rx, rw = 924, 368
    ptop, pbot = 44, 646
    ph = pbot - ptop

    panel(ax, lx, ptop, lw, ph, BLUE_BG, BLUE_ED)
    panel(ax, mx, ptop, mw, ph, ORANGE_BG, ORANGE_ED)
    panel(ax, rx, ptop, rw, ph, GREEN_BG, GREEN_ED)

    heading(ax, lx + lw / 2, pbot - 26, "Data inputs", BLUE_TX)
    heading(ax, mx + mw / 2, pbot - 26, "Reproducible calibration pipeline", ORANGE_TX)
    heading(ax, rx + rw / 2, pbot - 26, "Outputs", GREEN_TX)

    # Connecting arrows
    arrow(ax, lx + lw + 8, mx - 8, ptop + ph / 2)
    arrow(ax, mx + mw + 8, rx - 8, ptop + ph / 2)

    # LEFT: five input streams -------------------------------------------------
    inputs = [
        ("BT mobile OD records (licensed)", "feeds step 1",
         "Origin, destination, period, adult volume, inferred mode."),
        ("Official geography", "feeds step 1",
         "MSOA 2021 boundaries, centroids and regions of residence."),
        ("NTS mode targets", "feeds steps 2 and 3",
         "Mode shares by region and trip-length band (NTS 9916)."),
        ("Population base", "feeds steps 1 and 4",
         "Census and TfN NorMITs Land Use segments."),
        ("Trip-rate and purpose priors", "feeds steps 3 and 4",
         "NTS/TfN trip rates; NTS0502 period-purpose controls."),
    ]
    ch, gap = 96, 12
    top = pbot - 56
    for t, feeds, b in inputs:
        card(ax, lx + 14, top - ch, lw - 28, ch)
        title_body(ax, lx + 14, top - 16, t, b, 34)
        ax.text(lx + 30, top - ch + 14, feeds, ha="left", va="bottom",
                color=BLUE_TX, fontsize=8.4, fontweight="bold", zorder=3)
        top -= ch + gap

    # MIDDLE: five numbered steps ---------------------------------------------
    steps = [
        ("Standardise geography and\nbuild an all-age base",
         "Attach MSOA regions, centroids and trip-length bands; "
         "uplift adult volumes by local child/adult ratios."),
        ("Calibrate mode shares",
         "Match NTS 9916 targets by region and trip-length band."),
        ("Refine mode and time",
         "Split ROAD into road modes; constrain origin-period mode totals."),
        ("Allocate trip purpose",
         "Local trip-rate priors; optional NTS0502 raking."),
    ]
    sh, sgap = 108, 18
    top = pbot - 56
    for i, (t, b) in enumerate(steps, start=1):
        card(ax, mx + 14, top - sh, mw - 28, sh, ed=ORANGE_ED)
        cx = mx + 40
        cy = top - sh / 2
        ax.add_patch(Circle((cx, cy), 15, facecolor=ORANGE_DISC,
                            edgecolor="none", zorder=3))
        ax.text(cx, cy, str(i), ha="center", va="center", color="white",
                fontsize=12.5, fontweight="bold", zorder=4)
        title_body(ax, mx + 44, top - 18, t, b, 46, xpad=20)
        top -= sh + sgap

    # RIGHT: three output families --------------------------------------------
    outputs = [
        ("Synthetic OD matrices",
         "70 dense MSOA-to-MSOA matrices by mode, with purpose for the weekday AM peak.",
         ["Mode", "Purpose", "Period", "MSOA"]),
        ("Pipeline code",
         "The open uk-travel-pipeline source and command-line interface.",
         None),
        ("Documentation and metadata",
         "Schema notes, purpose-code mappings, licences and the options used for the release.",
         None),
    ]
    oh, ogap = 150, 20
    top = pbot - 56
    for t, b, tags in outputs:
        card(ax, rx + 16, top - oh, rw - 32, oh, ed=GREEN_ED)
        title_body(ax, rx + 16, top - 18, t, b, 40, tcolor=GREEN_TX,
                   tsize=12.5, bsize=9.6)
        if tags:
            tx = rx + 32
            ty = top - oh + 34
            for tg in tags:
                tx += tag(ax, tx, ty, tg, GREEN_TX) + 8
        top -= oh + ogap

    FIGURES_DIR.mkdir(exist_ok=True)
    fig.savefig(FIGURES_DIR / "figure1_overview.pdf", bbox_inches="tight",
                pad_inches=0.05)
    fig.savefig(FIGURES_DIR / "figure1_overview.png", dpi=300,
                bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)
    print("wrote figure1_overview.pdf and .png")


if __name__ == "__main__":
    build()
