import pandas as pd

from uk_travel_pipeline.local_authority import (
    TRIPS_COL,
    _add_trip_volume_columns,
    _period_profile_partition,
)


def test_add_trip_volume_columns_uses_weekday_and_weekend_multipliers():
    df = pd.DataFrame(
        {
            "volume_adj_purpose": [50.0, 10.0, 5.0],
            "days_used": [5, 2, 0],
            "weekend_flag": [False, True, False],
        }
    )

    out = _add_trip_volume_columns(df, "volume_adj_purpose", "purpose_daily_volume", TRIPS_COL)

    assert out["purpose_daily_volume"].tolist() == [10.0, 5.0, 0.0]
    assert out[TRIPS_COL].tolist() == [50.0, 10.0, 0.0]


def test_period_profile_partition_buckets_weekend_before_time_period():
    df = pd.DataFrame(
        {
            "time_period": ["AM_peak", "PM_peak", "off_peak", "AM_peak"],
            "weekend_flag": [False, False, False, True],
        }
    )

    out = _period_profile_partition(df)

    assert out["period_profile"].tolist() == [
        "weekday_AM",
        "weekday_PM",
        "weekday_off_peak",
        "weekend",
    ]
    assert out["day_type"].tolist() == ["weekday", "weekday", "weekday", "weekend"]
