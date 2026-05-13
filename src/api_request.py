import requests
import json
import os
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

API_URL = "https://api.sejm.gov.pl/sejm/term"
DELAY = 0.2


def fetch(url: str) -> dict | list:
    response = requests.get(url)
    response.raise_for_status()
    time.sleep(DELAY)
    return response.json()


def fetch_all_terms():
    return list(map(lambda x: x["num"], fetch(API_URL)))


def fetch_all_mps_per_term(term):
    return fetch(f"{API_URL}{term}/MP")


def fetch_all_proceedings_per_term(term):
    return list(map(lambda x: x["number"], fetch(f"{API_URL}{term}/proceedings")))


def fetch_all_votings_per_proceeding_per_term(term, proceeding):
    return list(map(lambda x: x["votingNumber"], fetch(f"{API_URL}{term}/votings/{proceeding}")))


def fetch_all_votes_per_voting_per_proceeding_per_term(term, proceeding, vote_num):
    return fetch(f"{API_URL}{term}/votings/{proceeding}/{vote_num}")


def save_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def already_fetched(path):
    if os.path.exists(path):
        log.info(f"Skipping — already exists: {path}")
        return True
    return False


if __name__ == "__main__":
    terms = fetch_all_terms()
    log.info(f"Found terms: {terms}")

    for term in terms:
        log.info(f"--- Term {term} ---")

        mps_path = f"../data/raw/mps/{term}/term{term}_mps.json"
        if not already_fetched(mps_path):
            mps = fetch_all_mps_per_term(term)
            save_json(mps, mps_path)
            log.info(f"MPs saved: {len(mps)}")

        proceedings = fetch_all_proceedings_per_term(term)
        log.info(f"Proceedings found: {len(proceedings)}")

        for proceeding in proceedings:
            vote_nums = fetch_all_votings_per_proceeding_per_term(term, proceeding)
            skipped = 0

            for vote_num in vote_nums:
                path = f"../data/raw/votings/{term}/proceeding/term{term}_proceeding{proceeding}_vote{vote_num}.json"

                if already_fetched(path):
                    skipped += 1
                    continue

                votes = fetch_all_votes_per_voting_per_proceeding_per_term(term, proceeding, vote_num)
                save_json(votes, path)

            fetched = len(vote_nums) - skipped
            log.info(f"  Proceeding {proceeding}: {fetched} fetched, {skipped} skipped ✓")