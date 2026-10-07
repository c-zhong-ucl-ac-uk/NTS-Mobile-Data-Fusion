# An open dataset of survey-calibrated origin-destination matrices for England and Wales

**Final adjusted matrices for a typical week and weekday AM peak, with purpose disaggregation for the AM peak**

## Release information

| Item | Details |
| --- | --- |
| Creators (as recorded on Zenodo) | Zhang, Bowen; Zhong, Cheng; Ma, Mingfei |
| Version | V1.0 |
| Publication date | 6 September 2026 |
| Dataset record and downloads | [Zenodo record 22546252](https://zenodo.org/records/22546252) |
| Dataset DOI | [10.5281/zenodo.22546252](https://doi.org/10.5281/zenodo.22546252) |
| Data licence | [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) |
| Reference period | September 2024–September 2025 |
| Geography | 2021 Middle layer Super Output Areas (MSOAs), England and Wales |
| Spatial coverage | 7,264 MSOAs: 6,856 in England and 408 in Wales |
| Format | Comma-separated values (CSV), with one origin–destination matrix per file |

| Creator (Zenodo metadata) | Affiliation |
| --- | --- |
| Zhang, Bowen | University College London |
| Zhong, Cheng | University College London |
| Ma, Mingfei | England's Economic Heartland Business Unit, Buckinghamshire Council |

## Overview

This dataset provides estimated origin–destination (OD) trip volumes between MSOAs in England and Wales. It combines aggregate mobile network mobility data supplied by BT with National Travel Survey (NTS), population and trip-production evidence. The outputs support analysis of travel patterns by mode and, for the weekday morning peak, by trip purpose.

The deposit contains two complementary final products:

| Filename prefix | Contents | Unit of each cell | Processing applied |
| --- | --- | --- | --- |
| `typical_week_` | Seven adjusted matrices, one per mode, covering all purposes and all daily time periods | Estimated person trips per representative seven-day week | Child-origin uplift, regional/distance-band mode calibration and road-mode splitting |
| `weekday_AMpeak_` | Seven adjusted all-purpose matrices and 56 adjusted purpose-specific matrices: seven modes × eight purposes | Estimated person trips per average single weekday AM peak, 07:00–09:59 | Child-origin uplift, regional/distance-band mode calibration, road-mode splitting and mode/time refinement; purpose-specific files also include purpose allocation and NTS0502 purpose raking |

**Only adjusted matrix outputs are distributed.** The deposit contains no raw BT source data, individual trajectories, unadjusted `*_raw.csv` matrices or intermediate trip-record files. Both the typical-week and AM-peak matrices are final adjusted outputs for their respective temporal definitions.

## Files and naming conventions

The Zenodo deposit contains 70 matrix CSV files and this README in a single file list. Each matrix filename includes its time period, so the files can be downloaded and used without a folder hierarchy. The period prefixes also distinguish the all-purpose matrices, which otherwise share the same filenames in the pipeline outputs.

| Filename pattern | Number of files | Contents |
| --- | --- | --- |
| `typical_week_OD_matrix_{MODE}_adjusted.csv` | 7 | Final all-purpose typical-week matrices |
| `weekday_AMpeak_OD_matrix_{MODE}_adjusted.csv` | 7 | Final all-purpose weekday AM-peak matrices |
| `weekday_AMpeak_OD_matrix_{MODE}_adjusted_by_purpose{N}.csv` | 56 | Final weekday AM-peak matrices disaggregated by purpose |

`{MODE}` is one of the seven mode codes below, and `{N}` is a purpose code from 1 to 8. These placeholders describe the naming convention; they are replaced by actual codes in each filename.

For example, `typical_week_OD_matrix_PRIVATE_CAR_adjusted.csv` contains all-purpose private-car-group flows for a representative week. `weekday_AMpeak_OD_matrix_PRIVATE_CAR_adjusted_by_purpose1.csv` contains estimated commuting flows for the same mode group during an average weekday AM peak.

## CSV structure and values

Each file is a square matrix of 7,264 origin zones by 7,264 destination zones, plus a header row and an origin-code column.

| CSV element | Meaning |
| --- | --- |
| First column, `origin_msoa` | Origin MSOA 2021 code |
| Remaining column headers | Destination MSOA 2021 codes |
| Numerical cell | Estimated trips from the row's origin to the column's destination for the mode, period and purpose identified by the filename |
| Diagonal cell | Trips whose origin and destination are within the same MSOA |

MSOA codes are identifiers and should be read as text. Origins and destinations use the same ordered zone list. Direction matters: the flow from A to B need not equal the flow from B to A. Row sums represent departures from an origin; column sums represent arrivals at a destination. Include diagonal cells when calculating totals unless the analysis specifically excludes within-MSOA trips.

Values are non-integer person-trip estimates produced by weighting and calibration. They are neither unique traveller counts nor vehicle counts. Retain the decimal values when aggregating.

The CSV exports contain both explicit zeros and empty fields. Empty fields arise where an OD combination has no entry in the pivoted export; the exporter also inserts zeros when adding unrepresented rows or columns. For numerical aggregation of these matrices, empty fields may be treated as zero contribution. Neither an empty field nor a zero establishes that no real-world travel occurred between the two zones.

## Travel modes

| Filename code | Description |
| --- | --- |
| `WALKING` | Walking |
| `CYCLE` | Pedal cycle |
| `PRIVATE_CAR` | Car/van driver, car/van passenger and other private transport, combined in the road-mode split |
| `MOTORCYCLE` | Motorcycle |
| `BUS` | Bus, combining London, other local and non-local bus categories |
| `RAIL` | Rail group; the NTS calibration combines surface rail and other public transport |
| `SUBWAY` | Subway/underground; calibrated using the NTS London Underground category |

These are harmonised output groups. Four road submodes—`CYCLE`, `PRIVATE_CAR`, `MOTORCYCLE` and `BUS`—are estimated by splitting the source `ROAD` category using NTS regional and distance-band shares. They are not separately observed road modes in the original mobile product.

## Trip purposes

Purpose-specific files are supplied only for the weekday AM-peak product.

| Code `{N}` | Description |
| --- | --- |
| 1 | Commuting |
| 2 | Employer business |
| 3 | Education |
| 4 | Shopping |
| 5 | Personal business |
| 6 | Social/leisure |
| 7 | Visiting family and friends |
| 8 | Holiday/day trip |

Purpose is estimated using population-weighted trip-production rates and survey controls; it is not directly observed in the mobile data. The NTS0502 controls combine purposes 6 and 7 into a single social/visiting-friends-and-relatives group, with their separate shares informed by the local purpose priors.

For a given mode and OD pair, summing purpose files 1–8 should recover the corresponding all-purpose adjusted AM-peak value, subject to floating-point precision and consistent treatment of empty fields. The all-purpose file is a total, not an additional purpose: do not add it to the eight purpose files.

## Temporal definitions

### Typical week

The typical-week product includes all source daily time periods: AM peak, PM peak and off-peak. Each adjusted source-record volume is divided by its own number of observed days (`days_used`), then multiplied by five for a weekday record or two for a weekend record. Contributions are summed for each OD pair and mode:

```text
Typical-week trips = sum over contributing records of:
    adjusted source-period volume / days_used × day-type weight

day-type weight = 5 for weekdays; 2 for weekend days
```

This represents five average weekdays plus two average weekend days. It is a weekly total, not an average day, a specific calendar week or the total over the observation window.

### Weekday AM peak

The AM-peak product includes weekday trips departing from the origin between 07:00 and 09:59. For the all-purpose matrices, each contributing adjusted source-record volume is divided by `days_used`. Purpose-specific matrices apply the same normalization to purpose-allocated volumes. These daily contributions are summed within each OD/mode/purpose combination.

Each value therefore represents one average weekday morning peak. It is not multiplied by five and is not an hourly rate. Five comparable weekday morning peaks can be represented by multiplying an AM-peak value by five, but this does not recreate an all-day weekly value.

## Methods, code and reproducibility

The associated methods paper is *Travel Mode- and Purpose-Specific Origin-Destination Matrices for England and Wales from Fused Travel Survey and Mobile Network Data*, by Bowen Zhang, Chen Zhong, Mingfei Ma and James Golding-Graham. It is available as [arXiv:2609.36466](https://arxiv.org/abs/2609.36466), with v1 submitted on 29 September 2026 ([PDF](https://arxiv.org/pdf/2609.36466)).

The processing code is available in the [NTS-Mobile-Data-Fusion repository](https://github.com/c-zhong-ucl-ac-uk/NTS-Mobile-Data-Fusion). Use the two reproduction commands in the repository README to generate the final typical-week and weekday AM-peak products.

The deposited matrices can be analysed without access to the source inputs. Rebuilding them from the original observations requires the source datasets, including appropriately licensed BT aggregate data, which are not included in this deposit.

## Licence and citation

The distributed matrix files are available under [Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/). When reusing the data, give appropriate credit, link to the licence and indicate any changes. The software licence is separate from the licence for these data.

Please cite the dataset version used, using the publication date and version recorded on Zenodo. A dataset reference is:

> Zhang, B., Zhong, C., & Ma, M. (2026). *An open dataset of survey-calibrated origin-destination matrices for England and Wales* (V1.0) [Data set]. Zenodo. [10.5281/zenodo.22546252](https://doi.org/10.5281/zenodo.22546252).

Please also cite the associated methods paper:

> Zhang, B., Zhong, C., Ma, M., & Golding-Graham, J. (2026). *Travel Mode- and Purpose-Specific Origin-Destination Matrices for England and Wales from Fused Travel Survey and Mobile Network Data*. [arXiv:2609.36466](https://arxiv.org/abs/2609.36466).

The dataset creator list above follows the Zenodo record; the paper has its own author list. A BibTeX entry for the paper is provided in the [repository README](https://github.com/c-zhong-ucl-ac-uk/NTS-Mobile-Data-Fusion#citation).
