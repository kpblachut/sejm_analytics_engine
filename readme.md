# Sejm Voting Data Analysis

This project processes and analyzes voting data from the Polish Sejm using Python and PySpark.

## Overview

The goal is to build a simple data pipeline that:
- fetches raw MP and voting data from the Sejm API
- stores raw JSON files in `data/raw/`
- transforms raw JSON into structured Parquet datasets in `data/silver/`
- enables further analysis of voting behavior

## Data Pipeline

API → raw JSON → Spark transformation → structured dataset (Parquet)

## Data Layout

- `data/raw/mps/` — raw MP data per term, e.g. `term10_mps.json`
- `data/raw/votings/` — raw voting session JSON files, organized by term and proceeding
- `data/silver/mps/` — cleaned MP Parquet dataset partitioned by `term`
- `data/silver/votings/` — cleaned votes Parquet dataset partitioned by `term`

## Data Contents

MP files include fields such as:
- `id`, `firstName`, `lastName`, `club`, `birthDate`, `districtName`, `educationLevel`, `profession`, `term`

Voting files include fields such as:
- `MP`, `vote`, plus metadata extracted from the filename: `term`, `proceeding`, `vote_id`

## Processing

The current transformation script is `src/file_proccesor.py`.
It:
- reads raw JSON recursively from `data/raw/mps/` and `data/raw/votings/`
- extracts metadata from input file paths
- renames and casts selected fields
- writes cleaned data to `data/silver/mps` and `data/silver/votings` in Parquet format

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

3. Inspect the generated Parquet datasets in `data/silver/`.

## Current Status

Work in progress.

Next steps include:
- improving MP identity modeling across terms
- adding analytical queries (e.g. voting similarity, club cohesion)
- adding more structured output and reporting