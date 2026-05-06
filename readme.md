# Sejm Voting Data Analysis

This project processes and analyzes voting data from the Polish Sejm using Python and PySpark.

## Overview

The goal is to build a simple data pipeline that:
- fetches raw voting data from the Sejm API
- stores it as JSON files
- transforms it into a structured format using PySpark
- enables further analysis of voting behavior

## Data Pipeline

API → raw JSON → Spark transformation → structured dataset

## Data

Each JSON file represents a single voting session and contains a list of MPs with their votes.

Example fields:
- MP (ID within term)
- firstName, lastName
- club
- vote (YES / NO / ABSTAIN)

Additional metadata (extracted from file name):
- term
- proceeding
- voting number

## Processing

Main steps:
- read JSON files using Spark
- extract metadata from file paths
- flatten nested structures
- build a tabular dataset of votes

Resulting structure (simplified):

term | proceeding | voting_id | MP | vote | club

## Tech Stack

- Python
- PySpark
- JSON API (Sejm)

## Status

Work in progress.  
Next steps include:
- improving data modeling (MP identity across terms)
- adding analytical queries (e.g. voting similarity)
- storing processed data in Parquet format

## How to run

1. Fetch data using the API script
2. Run Spark transformation script on raw data directory
3. Explore resulting dataset or extend transformations