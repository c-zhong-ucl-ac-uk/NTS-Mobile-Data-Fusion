from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

NTS0502_PURPOSE_SHEET = "NTS0502b_purpose_by_start_time"
NTS0502_ALL_DAY_SHEET = "NTS0502a_start_time_by_purpose"

SOURCE_PURPOSE_TO_CONTROL_GROUP = {
    "Commuting": "commuting",
    "Business": "business",
    "Education": "education",
    "Escort education": "education",
    "Shopping": "shopping",
    "Other work, other escort and personal business": "personal_business",
    "Visiting friends, entertainment and sport": "social_vfr",
    "Holiday, day trip and other": "holiday",
}

NTS0502_PERIOD_HOURS = {
    "weekday_AM": ["0700 to 0759", "0800 to 0859", "0900 to 0959"],
    "weekday_PM": ["1600 to 1659", "1700 to 1759", "1800 to 1859"],
    "weekday_off_peak": [
        "0000 to 0059",
        "0100 to 0159",
        "0200 to 0259",
        "0300 to 0359",
        "0400 to 0459",
        "0500 to 0559",
        "0600 to 0659",
        "1000 to 1059",
        "1100 to 1159",
        "1200 to 1259",
        "1300 to 1359",
        "1400 to 1459",
        "1500 to 1559",
        "1900 to 1959",
        "2000 to 2059",
        "2100 to 2159",
        "2200 to 2259",
        "2300 to 2359",
    ],
}


def _latest_year_label(labels: pd.Series) -> str:
    candidates = [str(label).strip() for label in labels.dropna().unique()]
    if not candidates:
        raise ValueError("NTS0502 workbook does not contain any year labels.")

    def sort_key(label: str) -> int:
        years = re.findall(r"\d{4}", label)
        return int(years[-1]) if years else -1

    return max(candidates, key=sort_key)


def _as_number(value: object) -> float:
    if isinstance(value, str) and value.strip().lower() == "[low]":
        return 0.0
    numeric = pd.to_numeric(value, errors="coerce")
    return 0.0 if pd.isna(numeric) else float(numeric)


def build_nts0502_period_purpose_controls(ods_path: Path, year: str | None = None) -> pd.DataFrame:
    purpose_by_time = pd.read_excel(
        ods_path,
        sheet_name=NTS0502_PURPOSE_SHEET,
        engine="odf",
        header=5,
    )
    time_by_purpose = pd.read_excel(
        ods_path,
        sheet_name=NTS0502_ALL_DAY_SHEET,
        engine="odf",
        header=5,
    )

    year_col = "Year [note 2]"
    if year is None:
        year = _latest_year_label(purpose_by_time[year_col])

    purpose_rows = purpose_by_time[purpose_by_time[year_col].astype(str).str.strip() == year].copy()
    purpose_rows = purpose_rows[purpose_rows["Trip purpose"].isin(SOURCE_PURPOSE_TO_CONTROL_GROUP)]
    if purpose_rows.empty:
        raise ValueError(f"NTS0502 sheet {NTS0502_PURPOSE_SHEET} has no rows for year {year!r}.")

    all_day = time_by_purpose[
        (time_by_purpose[year_col].astype(str).str.strip() == year)
        & (time_by_purpose["Start time"].astype(str).str.strip() == "All day")
    ]
    if all_day.empty:
        raise ValueError(f"NTS0502 sheet {NTS0502_ALL_DAY_SHEET} has no All day row for year {year!r}.")
    all_day = all_day.iloc[0]

    purpose_all_day_share = {
        "Commuting": _as_number(all_day["Commuting (%)"]) / 100.0,
        "Business": _as_number(all_day["Business (%)"]) / 100.0,
        "Education": _as_number(all_day["Education (%)"]) / 100.0,
        "Escort education": _as_number(all_day["Escort education (%)"]) / 100.0,
        "Shopping": _as_number(all_day["Shopping (%)"]) / 100.0,
        "Other work, other escort and personal business": _as_number(
            all_day["Other work, other escort and personal business (%)"]
        )
        / 100.0,
        "Visiting friends, entertainment and sport": _as_number(
            all_day["Visiting friends, entertainment and sport (%)"]
        )
        / 100.0,
        "Holiday, day trip and other": _as_number(all_day["Holiday, day trip and other (%)"]) / 100.0,
    }

    rows: list[dict[str, object]] = []
    for purpose_name, control_group in SOURCE_PURPOSE_TO_CONTROL_GROUP.items():
        purpose_row = purpose_rows[purpose_rows["Trip purpose"] == purpose_name].iloc[0]
        for period_key, hours in NTS0502_PERIOD_HOURS.items():
            source_period_share = sum(_as_number(purpose_row[f"{hour} (%)"]) / 100.0 for hour in hours)
            rows.append(
                {
                    "purpose_period_key": period_key,
                    "control_group": control_group,
                    "source_purpose": purpose_name,
                    "joint_share": purpose_all_day_share[purpose_name] * source_period_share,
                    "source_year": year,
                }
            )

    joint = pd.DataFrame(rows)
    controls = (
        joint.groupby(["purpose_period_key", "control_group", "source_year"], as_index=False)["joint_share"]
        .sum()
        .rename(columns={"joint_share": "joint_period_purpose_share"})
    )
    period_total = controls.groupby("purpose_period_key")["joint_period_purpose_share"].transform("sum")
    controls["control_share"] = (controls["joint_period_purpose_share"] / period_total).where(period_total > 0, 0.0)
    controls["period_trip_share"] = period_total
    return controls[
        [
            "purpose_period_key",
            "control_group",
            "control_share",
            "period_trip_share",
            "joint_period_purpose_share",
            "source_year",
        ]
    ].sort_values(["purpose_period_key", "control_group"])
