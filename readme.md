# Sejm Voting Data Analysis

This project processes and analyzes voting data from the Polish Sejm using Python and PySpark.

## Overview

The goal is to build a simple data pipeline that:
- fetches raw MP and voting data from the Sejm API
- stores raw JSON files in `data/raw/`
- transforms raw JSON into structured Parquet datasets in `data/silver/`
- enables further analysis of voting behavior

## Data Pipeline

API → raw JSON → Spark transformation → silver structured dataset (Parquet)

## Data Layout

- `data/raw/mps/` — raw MP data per term, typically named `term<term>_mps.json`
- `data/raw/votings/` — raw voting session JSON files organized by term and proceeding
- `data/silver/mps/` — cleaned MP Parquet dataset partitioned by `term`
- `data/silver/voting_meta/` — cleaned vote metadata Parquet dataset partitioned by `term`
- `data/silver/votes/` — normalized vote records Parquet dataset partitioned by `term`
- `data/silver/votes_on_list/` — normalized list-vote records Parquet dataset partitioned by `term`
 - `data/gold/party_lines/` — aggregated party-level metrics partitioned by `term`
 - `data/gold/loyalty_facts/` — per-vote MP facts with party context partitioned by `term`
 - `data/gold/mp_vs_all_parties/` — per-MP agreement stats against parties partitioned by `term`
 - `data/gold/mp_career/` — per-MP term summaries partitioned by `term`

## Raw Data Structure

### `data/raw/mps/`
Each MP JSON file is an array of MP objects for a given term. Common fields include:
- `id` — MP identifier
- `firstName`, `lastName`
- `birthDate`, `birthLocation`
- `club` — political club or caucus
- `active` — boolean active status
- `districtName`, `districtNum`
- `educationLevel`, `profession`
- `numberOfVotes` — total number of votes recorded for that MP
- `voivodeship`
- `term`

### `data/raw/votings/`
Each voting JSON file contains voting session metadata and a `votes` array. Key fields include:
- `term`
- `sitting`, `sittingDay`
- `votingNumber` — vote identifier
- `title`, `topic`, `description`, `kind`
- `majorityType`, `majorityVotes`
- `yes`, `no`, `abstain`, `notParticipating`, `present`, `totalVoted`
- `votes` — array of vote entries, usually containing:
  - `MP` — MP identifier
  - `club`
  - `vote` — raw vote label such as `YES`, `NO`, `ABSTAIN`, `ABSENT`
  - `listVotes` — optional object for list-based vote options

## Silver Data Structure

### `data/silver/mps/`
The cleaned MP Parquet table contains one row per MP with:
- `person_id` — normalized identity hash based on name and birth date
- `mp_id` — raw MP identifier
- `first_name`, `last_name`
- `birth_date`, `birth_location`
- `club`
- `active`
- `district_name`, `district_num`
- `education_level`, `profession`
- `number_of_votes`
- `voivodeship`
- `term`

### `data/silver/voting_meta/`
The voting metadata Parquet table contains one row per vote session with:
- `term`
- `sitting`
- `vote_id`
- `sitting_day`
- `date`
- `title`
- `topic`
- `description`
- `kind`
- `majority_type`
- `majority_vote`
- `yes`, `no`, `abstain`, `not_participating`, `present`, `total_voted`

### `data/silver/votes/`
The normalized vote records Parquet table contains one row per MP vote with:
- `term`
- `sitting`
- `vote_id`
- `mp_id`
- `club_at_vote`
- `vote_raw`
- `vote_normalized` — standardized to `YES`, `NO`, `ABSTAIN`, `ABSENT`, or `OTHER`
- `did_vote` — boolean indicating whether the MP voted as `YES`, `NO`, or `ABSTAIN`

### `data/silver/votes_on_list/`
The list-vote records Parquet table contains one row per MP vote option when `listVotes` are present, with:
- `term`
- `sitting`
- `vote_id`
- `mp_id`
- `club_at_vote`
- `option_number`
- `vote_raw`
- `vote_normalized`
- `did_vote`

## Gold Data Structure

The `src/build_gold.py` script produces several gold tables written under `data/gold/`. These are summary and analytical tables derived from the silver datasets.

### `data/gold/party_lines`
Per-term and per-vote aggregated party-level counts and cohesion metrics (one row per `term` / `sitting` / `vote_id` / `club_at_vote`):
 - `term`, `sitting`, `vote_id`
 - `club_at_vote` — party/club being evaluated
 - `yes_count`, `no_count`, `abstain_count`, `absent_count`, `present_count`, `other_count` — raw counts per category
 - `voted_count` — number of MPs who cast a YES/NO/ABSTAIN
 - `total_count` — total MPs in the group (including ABSENT/PRESENT/OTHER)
 - `max_votes` — max of `yes_count`, `no_count`, `abstain_count`
 - `cohesion_voted` — percentage (0-100) of the dominant vote among those who voted (`max_votes / voted_count`)
 - `cohesion_all` — percentage (0-100) of the dominant vote among all MPs in the group (`max_votes / total_count`)
 - `n_max_categories` — number of categories tied for `max_votes`
 - `party_line_simple` — dominant category (`YES` / `NO` / `ABSTAIN` / `TIE` / `NULL` if no votes)
 - `party_line_strict` — dominant line if `cohesion_voted > 70` and not a tie (otherwise `NULL`)

### `data/gold/loyalty_facts`
One row per MP vote event with MP attributes, vote, and party-line context (suitable for per-vote analytics):
 - `term`, `sitting`, `vote_id`, `date`, `title`, `topic`
 - `person_id`, `mp_id`, `first_name`, `last_name`, `birth_date`
 - `club_at_vote` — MP's club at time of the vote
 - `club_end_of_term` — MP's recorded club at term end (from `mps`)
 - `district_name`, `voivodeship`, `education_level`, `profession`, `number_of_votes`
 - `vote_normalized` — normalized vote label (`YES` / `NO` / `ABSTAIN` / `ABSENT` / `OTHER`)
 - `did_vote` — boolean (True if `YES`/`NO`/`ABSTAIN`)
 - party-level context fields copied from `party_lines`: `party_line_simple`, `party_line_strict`, `cohesion_voted`, `cohesion_all`
 - agreement indicators:
    - `agrees_simple_voted` — (nullable boolean) whether the MP's vote equals `party_line_simple`, only when the MP voted and `party_line_simple` is defined and not `TIE`
    - `agrees_simple_all` — boolean whether MP's vote equals `party_line_simple` (counts ABSENT as disagreement)
    - `agrees_strict_voted`, `agrees_strict_all` — analogous indicators for `party_line_strict`

### `data/gold/mp_vs_all_parties`
Per-MP agreement statistics comparing the MP to all evaluated party-lines across votes (one row per `person_id`, `term`, `club_at_vote`, `evaluated_club`):
 - `person_id`, `term`, `club_at_vote` (MP's club at vote time)
 - `evaluated_club` — the party that produced the evaluated `party_line_strict`
 - `n_agree` — number of times the MP agreed with that evaluated party line
 - `n_compared` — number of comparable votes considered (non-null comparisons)
 - `n_votes_total` — total votes in the join (including non-comparable)
 - `agreement_pct` — percentage (0-100) of agreements where comparison was possible

### `data/gold/mp_career`
Per-MP term-level summary (one row per `person_id` / `term`):
 - `person_id`, `term`, `mp_id`, `first_name`, `last_name`, `birth_date`
 - `voivodeship`, `education_level`, `profession`, `number_of_votes`, `club_end_of_term`
 - `n_votes_total` — total number of vote records in the term
 - `n_voted` — number of votes where the MP cast YES/NO/ABSTAIN
 - `n_absent` — number of votes recorded as `ABSENT`
 - `n_present_no_vote` — number recorded as present but not voting
 - loyalty summary percentages (0-100): `loyalty_simple_voted_pct`, `loyalty_simple_all_pct`, `loyalty_strict_voted_pct`, `loyalty_strict_all_pct`

These gold tables are written by `src/build_gold.py` to:
 - `data/gold/party_lines`
 - `data/gold/loyalty_facts`
 - `data/gold/mp_vs_all_parties`
 - `data/gold/mp_career`

## Quick examples — reading gold tables with PySpark

Start a Spark session and read a gold table, then run a simple query. Example in `python`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadGold").master("local[*]").getOrCreate()

# Read party_lines
df_party = spark.read.parquet("../data/gold/party_lines")
df_party.select("term", "vote_id", "club_at_vote", "cohesion_voted").show(5)

# Read loyalty_facts
df_loyalty = spark.read.parquet("../data/gold/loyalty_facts")
df_loyalty.filter("did_vote").select("term", "mp_id", "vote_normalized", "party_line_simple").show(5)

# Read mp_vs_all_parties
df_vs = spark.read.parquet("../data/gold/mp_vs_all_parties")
df_vs.orderBy(df_vs.agreement_pct.desc()).show(10)

# Read mp_career
df_career = spark.read.parquet("../data/gold/mp_career")
df_career.select("person_id", "term", "n_voted", "loyalty_simple_voted_pct").show(5)

spark.stop()
```

## Processing

The current transformation scripts are `src/file_proccesor.py` and `src/build_gold.py`.

`src/file_proccesor.py`:
- reads raw JSON recursively from `data/raw/mps/` and `data/raw/votings/`
- normalizes MP names and computes a stable `person_id`
- extracts voting metadata and explodes vote records
- normalizes raw vote labels and flags whether an MP participated
- writes cleaned Parquet datasets to `data/silver/`

`src/build_gold.py`:
- reads cleaned Parquet datasets from `data/silver/mps/`, `data/silver/votes/`, and `data/silver/voting_meta/`
- joins voting records with MP details on `term` and `mp_id`, aggregates party-level metrics, and computes per-MP loyalty statistics
- writes multiple gold tables to `data/gold/party_lines`, `data/gold/loyalty_facts`, `data/gold/mp_vs_all_parties`, and `data/gold/mp_career`

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

2. Transform raw JSON into silver Parquet datasets:
   ```bash
   python3 src/file_proccesor.py
   ```

3. Build enriched gold dataset:
   ```bash
   python3 src/build_gold.py
   ```

4. Inspect the generated Parquet datasets under `data/silver/` and `data/gold/`.

## Current Status

The pipeline currently produces raw, silver, and gold layers, with most transformation logic implemented in `src/file_proccesor.py`.

Next steps include:
- improving MP identity modeling across terms
- adding analytical queries (e.g. voting similarity, club cohesion)
- adding structured reporting and metrics