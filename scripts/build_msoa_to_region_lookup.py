#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

MSOA_CODE_COLS = ["MSOA21CD", "MSOA11CD"]
LAD_CODE_COLS = ["LAD24CD", "LAD23CD", "LAD22CD", "LAD21CD"]
LAD_CODE_LOOKUP_COLS = LAD_CODE_COLS + ["LA code", "la_code"]
REGION_COLS = [
    "Region of residence",
    "Region name",
    "RGN24NM",
    "RGN23NM",
    "RGN22NM",
    "RGN21NM",
    "RGN11NM",
    "region_name",
    "region",
]

REGION_NAME_NORMALIZATION = {
    "east": "East of England",
    "east of england": "East of England",
    "north east": "North East",
    "north west": "North West",
    "yorkshire and the humber": "Yorkshire and The Humber",
    "east midlands": "East Midlands",
    "west midlands": "West Midlands",
    "south east": "South East",
    "south west": "South West",
    "london": "London",
    "wales": "Wales",
}


def _pick_column(columns: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


def _load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _load_excel(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, sheet_name=0, header=None)
    header_idx = None
    for i in range(min(len(raw), 50)):
        vals = [str(v).strip() for v in raw.iloc[i].tolist() if str(v).strip() and str(v).strip() != "nan"]
        if "LA code" in vals and "Region name" in vals:
            header_idx = i
            break
    if header_idx is None:
        df = pd.read_excel(path, sheet_name=0)
    else:
        df = pd.read_excel(path, sheet_name=0, header=header_idx)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _load_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in {".csv", ".txt"}:
        return _load_csv(path)
    if ext in {".xlsx", ".xlsm", ".xls"}:
        return _load_excel(path)
    raise ValueError(f"Unsupported file type for {path}. Use CSV or Excel.")


def _normalize_region_name(value: object) -> str:
    raw = str(value).strip()
    key = " ".join(raw.split()).lower()
    return REGION_NAME_NORMALIZATION.get(key, raw)


def build_lookup(
    msoa_lookup_csv: Path,
    output_csv: Path,
    lad_region_lookup_csv: Path | None = None,
    default_region: str | None = None,
) -> Path:
    msoa_df = _load_table(msoa_lookup_csv)
    msoa_col = _pick_column(msoa_df.columns.tolist(), MSOA_CODE_COLS)
    if msoa_col is None:
        raise ValueError(f"{msoa_lookup_csv} must include one of {MSOA_CODE_COLS}.")

    region_col = _pick_column(msoa_df.columns.tolist(), REGION_COLS)
    if region_col is not None:
        out = msoa_df[[msoa_col, region_col]].copy()
    else:
        lad_col = _pick_column(msoa_df.columns.tolist(), LAD_CODE_COLS)
        if lad_col is None and default_region is None:
            raise ValueError(
                f"{msoa_lookup_csv} has no region column ({REGION_COLS}) and no LAD code ({LAD_CODE_COLS})."
            )
        if lad_col is None and default_region is not None:
            out = msoa_df[[msoa_col]].copy()
            out["Region of residence"] = default_region
        else:
            if lad_region_lookup_csv is None:
                raise ValueError(
                    "LAD code found in MSOA lookup, but --lad-region-lookup-csv was not provided."
                )
            lad_df = _load_table(lad_region_lookup_csv)
            lad_code_col = _pick_column(lad_df.columns.tolist(), LAD_CODE_LOOKUP_COLS)
            lad_region_col = _pick_column(lad_df.columns.tolist(), REGION_COLS)
            if lad_code_col is None or lad_region_col is None:
                raise ValueError(
                    f"{lad_region_lookup_csv} must include one LAD code column ({LAD_CODE_LOOKUP_COLS}) "
                    f"and one region column ({REGION_COLS})."
                )
            out = msoa_df[[msoa_col, lad_col]].merge(
                lad_df[[lad_code_col, lad_region_col]],
                left_on=lad_col,
                right_on=lad_code_col,
                how="left",
            )[[msoa_col, lad_region_col]]
            if default_region is not None:
                out[lad_region_col] = out[lad_region_col].fillna(default_region)

    out = out.rename(columns={msoa_col: "MSOA21CD", out.columns[1]: "Region of residence"})
    out["MSOA21CD"] = out["MSOA21CD"].astype(str).str.strip()
    out["Region of residence"] = out["Region of residence"].map(_normalize_region_name)
    out = out[(out["MSOA21CD"] != "") & (out["Region of residence"] != "")].drop_duplicates("MSOA21CD")
    out = out.sort_values("MSOA21CD").reset_index(drop=True)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)
    return output_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build msoa_to_region.csv from official lookup CSV/Excel files.")
    p.add_argument("--msoa-lookup-csv", type=Path, required=True)
    p.add_argument("--output-csv", type=Path, default=Path("data/raw/lookups/msoa_to_region.csv"))
    p.add_argument("--lad-region-lookup-csv", type=Path, default=None)
    p.add_argument(
        "--default-region",
        default=None,
        help="Optional fallback region name for rows where region cannot be resolved.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = build_lookup(
        msoa_lookup_csv=args.msoa_lookup_csv,
        output_csv=args.output_csv,
        lad_region_lookup_csv=args.lad_region_lookup_csv,
        default_region=args.default_region,
    )
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
