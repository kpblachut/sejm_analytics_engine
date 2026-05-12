# Sejm Voting Data Analysis

This project processes and analyzes voting data from the Polish Sejm using Python and PySpark.

## Overview

The goal is to build a simple data pipeline that:
- fetches raw MP and voting data from the Sejm API
- stores raw JSON files in `data/raw/`
- transforms raw JSON into structured Parquet datasets in `data/silver/`
- enriches voting data by joining with MP information in `data/gold/`
- enables further analysis of voting behavior

## Data Pipeline

API → raw JSON → Spark transformation → silver structured dataset (Parquet) → gold enriched dataset (Parquet)

## Data Layout

- `data/raw/mps/` — raw MP data per term, e.g. `term10_mps.json`
- `data/raw/votings/` — raw voting session JSON files, organized by term and proceeding
- `data/silver/mps/` — cleaned MP Parquet dataset partitioned by `term`
- `data/silver/votings/` — cleaned votes Parquet dataset partitioned by `term`
- `data/gold/votes_enriched/` — enriched votes Parquet dataset partitioned by `term`

## Data Contents

MP files include fields such as:
- `id`, `firstName`, `lastName`, `club`, `birthDate`, `districtName`, `educationLevel`, `profession`, `term`

Voting files include fields such as:
- `MP`, `vote`, plus metadata extracted from the filename: `term`, `proceeding`, `vote_id`

## Processing

The current transformation scripts are `src/file_proccesor.py` and `src/build_gold.py`.

`file_proccesor.py`:
- reads raw JSON recursively from `data/raw/mps/` and `data/raw/votings/`
- extracts metadata from input file paths
- renames and casts selected fields
- writes cleaned data to `data/silver/mps` and `data/silver/votings` in Parquet format

`build_gold.py`:
- reads cleaned Parquet datasets from `data/silver/mps` and `data/silver/votings`
- joins voting data with MP data on `term` and `mp_id`
- writes enriched voting data to `data/gold/votes_enriched` in Parquet format

## Requirements

- Python 3
- `pyspark`
- `requests`

## Run Instructions

From the repository root:

1. Fetch raw data:
   ```bash
   python3 src/api_request.py
   ```

2. Transform raw JSON into Parquet:
   ```bash
   python3 src/file_proccesor.py
   ```

3. Build gold enriched dataset:
   ```bash
   python3 src/build_gold.py
   ```

4. Inspect the generated Parquet datasets in `data/silver/` and `data/gold/`.

## Current Status

Data pipeline implemented with raw, silver, and gold layers.

Next steps include:
- improving MP identity modeling across terms
- adding analytical queries (e.g. voting similarity, club cohesion)
- adding more structured output and reporting