# NTS–Mobile Data Fusion

Python code for *Travel Mode- and Purpose-Specific Origin–Destination Matrices for England and Wales from Fused Travel Survey and Mobile Network Data*. The `uk-travel-pipeline` package combines aggregate BT mobility records with National Travel Survey (NTS), population and trip-production evidence to estimate MSOA-to-MSOA travel by mode, time period and purpose.

- Code repository: [NTS-Mobile-Data-Fusion](https://github.com/c-zhong-ucl-ac-uk/NTS-Mobile-Data-Fusion)
- Released matrices: [Zenodo, DOI 10.5281/zenodo.22546252](https://doi.org/10.5281/zenodo.22546252)
- Dataset structure, units, mode/purpose definitions and citation: [README_Zenodo_V1.0.md](README_Zenodo_V1.0.md)

The deposit contains 70 adjusted matrix CSVs covering 7,264 MSOAs in England and Wales. Its typical-week and weekday AM-peak products come from different processing stages; the two build commands below preserve that distinction. This repository publishes code and documentation alongside existing public reference inputs. Licensed source data, omitted supporting inputs, processed trip records and generated matrices must be supplied or generated separately.

## Installation

Use Python 3.10 or later. Run commands from the repository root in a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

The core installation includes Dask, GeoPandas, NumPy, pandas, PyArrow and odfpy. Install the plotting and test dependencies when needed:

```bash
python -m pip install -e '.[plots,dev]'
python -m pytest
```

The `plots` extra adds Matplotlib and seaborn. The tests use small synthetic fixtures; they do not require the national BT dataset. National builds require substantial storage and memory, especially for the purpose-disaggregated intermediate and dense CSV matrices.

## Required inputs

Rebuilding the published results requires the matching source-data versions, including appropriately licensed aggregate BT data for September 2024–September 2025. Individual trajectories are not used or distributed. Population and trip-rate inputs are also omitted from this code release and must be obtained separately. Providing a different compatible feed or updated controls produces a new estimate, rather than reproducing the deposited values.

Place inputs at the following paths, or use the corresponding CLI override. Paths are relative to the repository root. Public reference files, including the MSOA boundaries, region lookup and NTS9916 workbook, may already be present in your checkout; check their availability and versions before building.

| Input | Default path | CLI override |
| --- | --- | --- |
| BT aggregate OD records | `data/raw/bt_api/bt_modal_share_All_UK_MSOA_2024_09_2025_09.parquet` | `--api-data-path` (alias `--bt-parquet`) |
| MSOA 2021 boundaries | `data/raw/geo/Middle_layer_Super_Output_Areas_December_2021_Boundaries_EW_BGC_V3_4916445166053426.geojson` | `--msoa-geojson` |
| MSOA-to-NTS-region lookup | `data/raw/lookups/msoa_to_region.csv` | `--msoa-region-lookup-path` |
| NTS9916 workbook | `data/raw/nts/nts9916.ods` | `--nts-file` |
| Population segments | `data/raw/trip_production_calculation_code/pop_lad_gb/pop_id.csv` | `--pop-lsoa-internal-csv` |
| TfN area types | `data/raw/trip_production_calculation_code/tfn_area_type_lsoa21.csv` | `--tfn-area-type-lsoa-csv` |
| LSOA-to-MSOA lookup | `data/raw/lookups/OA21_LSOA21_MSOA21_LAD21_EW_V3.csv` | `--lsoa-msoa-lookup-csv` |
| Trip-production rates | `data/raw/trip_production_calculation_code/01202 - NTS Trip Rates v23.0/mode_time_split_production_hb_fr_reg.csv` | `--nts-mode-time-split-csv` |
| Purpose descriptions | `data/raw/trip_production_calculation_code/01202 - NTS Trip Rates v23.0/data_definition/purposes.csv` | `--purposes-csv` |
| Prepared NTS0502 controls | `data/raw/nts/nts0502_period_purpose_controls.csv` | `--nts0502-period-purpose-csv` |

The BT parquet must contain `origin_msoa`, `destination_msoa`, `volume`, `days_used`, `journey_time_mean`, `mode_of_transport`, `time_period` and `weekend_flag`. Source modes are `ROAD`, `RAIL`, `WALKING` and `SUBWAY`; weekday AM records use `time_period=AM_peak` and `weekend_flag=0`. The boundaries require `MSOA21CD`, geometry and a valid CRS; `BNG_E` and `BNG_N` are used when available, otherwise geometry centroids are calculated in British National Grid.

The region lookup requires `MSOA21CD` and a recognised region column such as `Region of residence` or `RGN21NM`. It is automatically used when present at its default path; region information may alternatively be supplied in the boundaries. The population file uses LSOA21 codes as wide population columns and includes `gender_3`, `aws` and `hh_type`; child rows are identified by `gender_3 == 1` and `aws == 1`. Area types require `lsoa21_code` and `tfn_area_type`, and the LSOA/MSOA lookup requires `LSOA21CD` and `MSOA21CD`. Trip rates require `hh_type`, `tfn_at`, `mode`, `period`, `purpose`, `trips.est` and `rho`. Purpose descriptions use `Purpose` and `Description`.

Obtain geography from [ONS Open Geography](https://geoportal.statistics.gov.uk/), NTS9916 from the [NTS statistics collection](https://www.gov.uk/government/collections/national-travel-survey-statistics), and NTS0502 from [NTS trip tables](https://www.gov.uk/government/statistical-data-sets/nts05-trips). The manuscript uses TfN NTS Trip Rates v23.0 and [NorMITs Land Use population evidence](https://tfn-land-use-test.readthedocs.io/en/latest/population/data_sources.html). Its TfN trip-rate reference identifies `TfNOffer@transportforthenorth.com` as the contact for obtaining the tables.

Build a region lookup from an official lookup containing MSOA and region columns:

```bash
python scripts/build_msoa_to_region_lookup.py \
  --msoa-lookup-csv /path/to/official_msoa_lookup.csv \
  --output-csv data/raw/lookups/msoa_to_region.csv
```

If the first lookup contains LAD codes instead of regions, also pass `--lad-region-lookup-csv /path/to/lad_to_region.csv`.

If the prepared NTS0502 control CSV is absent, the following preparation step is required before the AM-peak build. Download the ODS workbook and explicitly select the paper's 2023–2024 survey period:

```bash
python scripts/build_nts0502_period_purpose_controls.py \
  --nts0502-ods /path/to/nts0502.ods \
  --year '2023 to 2024' \
  --output-csv data/raw/nts/nts0502_period_purpose_controls.csv
```

The builder reads `NTS0502b_purpose_by_start_time` and `NTS0502a_start_time_by_purpose`. Omitting `--year` selects the latest workbook label, which may differ from the paper. NTS9916 is read from `NTS9916a_trips_region`; its calibration year is set separately by `--year 2024` in the pipeline commands below.

## Reproduce the two deposited products

Run these commands only after obtaining and checking the inputs. They describe the paper's configuration; installation and tests alone do not rebuild the national data. Child-origin uplift is enabled in both builds. `--split-road-mode` produces the seven output groups: `CYCLE`, `PRIVATE_CAR`, `MOTORCYCLE`, `BUS`, `RAIL`, `WALKING` and `SUBWAY`.

Use new or clean run directories when changing options. Keep each build's adjusted parquet, purpose parquet and output directory together. Skipping a stage does not delete matrices or diagnostics left by an earlier run; in particular, `--skip-purpose-estimation` ignores an existing purpose parquet but does not remove old purpose CSVs. Preserve existing results before reusing their paths.

### 1. Typical week before mode/time refinement

This build applies the child-origin uplift, NTS9916 regional/distance-band calibration and road-mode splitting. It disables mode/time refinement and purpose allocation, and stores its intermediate and outputs separately:

```bash
uk-travel-pipeline run \
  --year 2024 \
  --split-road-mode \
  --skip-mode-time-constraint \
  --skip-purpose-estimation \
  --adjusted-parquet data/processed/reassign/trips_adjusted_step7.parquet \
  --outputs-root outputs_step7
```

The seven deposited typical-week matrices are:

```text
outputs_step7/matrices/typical_week_by_mode/OD_matrix_{MODE}_adjusted.csv
```

The command also creates raw matrices and AM-peak matrices at this earlier stage. Those additional files are not part of the deposit.

### 2. Weekday AM peak with purpose allocation and raking

This build enables mode/time refinement, local purpose allocation and NTS0502 weekday purpose raking. It uses the default intermediate and output locations expected by the paper's validation script:

```bash
uk-travel-pipeline run \
  --year 2024 \
  --split-road-mode \
  --purpose-calibration nts0502 \
  --nts0502-period-purpose-csv data/raw/nts/nts0502_period_purpose_controls.csv
```

The main intermediates are `data/processed/reassign/trips_adjusted.parquet` and `data/processed/reassign/trips_adjusted_by_purpose.parquet`. The deposited AM-peak files are the seven all-purpose matrices and 56 purpose-specific matrices:

```text
outputs/matrices/weekday_AMpeak_by_mode/OD_matrix_{MODE}_adjusted.csv
outputs/matrices/weekday_AMpeak_by_mode/OD_matrix_{MODE}_adjusted_by_purpose{N}.csv
```

Here `{N}` runs from 1 to 8. This command also generates typical-week matrices after refinement, including purpose allocations; they are not the deposited typical-week product.

For the flat Zenodo file list, prefix the seven files from build 1 with `typical_week_` and the 63 files from build 2 with `weekday_AMpeak_`. Select adjusted files only, excluding `*_raw.csv`. See [README_Zenodo_V1.0.md](README_Zenodo_V1.0.md) for the complete 70-file contract and temporal normalization: the typical-week product represents five average weekdays plus two average weekend days, while the AM product represents one average weekday 07:00–09:59 period.

## Diagnostics and paper figures

Each build writes diagnostics under its output root's `reassign/` directory, including `adjustment_factors.csv`, `child_origin_uplift_factors.csv` and `share_check.csv`. The full build additionally writes `mode_time_adjustment_factors.csv`, `mode_time_share_check.csv` and `nts0502_purpose_control_check.csv`. Run-level summary tables are written under `qa/`.

After completing build 2, regenerate the current manuscript's two-panel validation figure and supplementary workflow overview:

```bash
python -m pip install -e '.[plots]'
python paper/scripts/make_validation_figure.py --recompute
python paper/scripts/make_figure1_overview.py
```

These write figures under `paper/figures/`; generated figures are not bundled with the code release. The validation script reads `outputs/reassign/` and the default purpose parquet. Its mode panel reconstructs the pre-refinement calibration stage and checks it against the mode/time diagnostic. `--recompute` rebuilds the AM-purpose cache from the purpose parquet and checks it against the NTS0502 diagnostic. The script expects the paper's national build, including 427 region/band/mode cells with survey targets, and is not a general validation command for regional sensitivity runs.

## Other CLI uses

Run an individual stage with the same options as the corresponding full build:

```bash
uk-travel-pipeline reassign --split-road-mode --purpose-calibration nts0502
uk-travel-pipeline matrices --split-road-mode
```

For a regional run, pass `--msoa-filter-list-path /path/to/msoa_list.csv` with an `MSOA21CD` column; both endpoints are filtered to the list. Without this option, the CLI processes all zones in the supplied BT data, so ensure the source geography matches the intended study area. `--region` supplies a fallback NTS region and does not filter records geographically. Use explicit `--adjusted-parquet`, `--purpose-parquet` and `--outputs-root` paths for alternative runs.

Sensitivity options include `--skip-child-origin-uplift`, `--skip-mode-time-constraint` and `--skip-purpose-estimation`. The default purpose method, `--purpose-calibration mode_time_split`, keeps local priors without NTS0502 raking. The default mode set retains the broad `ROAD` category unless `--split-road-mode` is supplied. Thus a plain `uk-travel-pipeline run` does not reproduce either deposited product.

To additionally export adjusted trip records as numbered CSV parts, add `--adjusted-csv-dir outputs/adjusted_trip_csv` to `run`, `reassign` or `matrices`. These are intermediate trip records, not the released dense OD matrices. `--legacy-output` optionally writes compatibility outputs under `output/`.

Build the Luton–Dunstable and Luton Airport evidence extracts from existing adjusted outputs, then plot them:

```bash
uk-travel-pipeline local-authority-use-case
uk-travel-pipeline local-authority-visualisations
```

These commands use `data/raw/lookups/EEH-MSOACDs.csv` and an additional MSOA-to-LAD lookup that must be acquired separately: `data/raw/lookups/OA21_LAD22_LSOA21_MSOA21_LEP22_EN_LU_V2_6716459600479702985.csv`. Override these paths with `--eeh-msoa-lookup-csv` and `--msoa-lad-lookup-csv`. Results are written under `outputs/local_authority_use_cases/`. Inspect each command's `--help` output for other input-path overrides, MSOA study-area radii and British National Grid anchors. Plotting requires the `plots` extra.

For the complete option list, run `uk-travel-pipeline --help` or `uk-travel-pipeline <command> --help`.

## Licence

The software is distributed under the [MIT licence](LICENSE). The deposited matrices have a separate CC BY 4.0 licence; consult the [dataset README](README_Zenodo_V1.0.md) for attribution and citation. Source-data licences and access conditions continue to apply to the inputs.
