# Windows Distribution Guide

This guide describes the first doctor-friendly release flow for FAERS-DB.

## Release Shape

Ship two downloads:

1. `FAERS-DB-Windows.zip` - the app launcher and bundled Python runtime.
2. `faers-warehouse-<dataset-version>.zip` or `.7z` - the prebuilt Parquet warehouse.

Do not put the warehouse inside `FAERS-DB.exe`. Keeping it separate lets users update data without reinstalling the app.

## Build The Warehouse

On the maintainer machine:

```bash
uv run python -m faersdb build
uv run python -m faersdb validate-warehouse
uv run python -m faersdb write-manifest --dataset-version 2025q4 --source-url "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
```

The runtime warehouse must contain:

- `demo.parquet`, `drug.parquet`, `reac.parquet`, `outc.parquet`, `ther.parquet`, `indi.parquet`, `rpsr.parquet`
- `latest_demo.parquet`, `latest_drug.parquet`, `latest_reac.parquet`, `latest_outc.parquet`, `latest_ther.parquet`, `latest_indi.parquet`, `latest_rpsr.parquet`
- `case_summary.parquet`, `filter_metadata.parquet`
- `warehouse-manifest.json`

Archive the `warehouse/` folder after validation. Prefer `.7z` for smaller data bundles if your users are comfortable extracting it; otherwise use `.zip`.

## Build The Windows App

Build on Windows, not Linux, so the generated executable is a Windows binary:

```powershell
uv sync --group dev
uv run pyinstaller --clean --noconfirm faers-db-windows.spec
```

The output is `dist\FAERS-DB\FAERS-DB.exe`.

Create `FAERS-DB-Windows.zip` from the whole `dist\FAERS-DB` folder. Include a `warehouse` folder placeholder or a short `README-START-HERE.txt` with these instructions:

1. Extract `FAERS-DB-Windows.zip`.
2. Extract the released warehouse bundle.
3. Put the extracted `warehouse` folder beside `FAERS-DB.exe`, or put it at `%LOCALAPPDATA%\FAERS-DB\warehouse`.
4. Double-click `FAERS-DB.exe`.
5. Keep the launcher window open while using the browser app.

## Data Update Flow

For the first release, updates are manual:

1. Publish a new `faers-warehouse-<dataset-version>` archive.
2. Ask users to close FAERS-DB.
3. Ask users to replace their old `warehouse` folder with the new one.
4. Ask users to launch FAERS-DB again.

The app validates required Parquet files at startup and shows a clear error if the data bundle is missing or incompatible.

Later, this can become an in-app downloader that downloads a manifest, verifies checksums, extracts to a temporary folder, then swaps the active warehouse only after validation succeeds.

## Windows Release Test Checklist

Run this on a clean Windows 10/11 VM without Python installed:

- Double-click `FAERS-DB.exe` with no warehouse present and confirm the error explains where to put the data.
- Put the warehouse beside `FAERS-DB.exe` and confirm the browser opens to `/app`.
- Put the app in a path with spaces, such as `C:\Users\Doctor Name\Desktop\FAERS DB`, and repeat launch.
- Confirm the header reports data as ready and shows whether a manifest was found.
- Run a drug search, reaction search, case detail view, CSV export, and JSON case report export.
- Replace one required Parquet file with an invalid file and confirm startup fails clearly.
- Record startup time, first-query time, disk footprint, and approximate memory use.
- Scan the zip/exe with Windows Defender. If SmartScreen blocks distribution, plan for code signing.
