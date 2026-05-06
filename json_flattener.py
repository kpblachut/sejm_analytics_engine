import os
import json

path = "/home/kacper/dataeng/data/raw/votings"

for root, _, files in os.walk(path):
    for filename in files:
        file_path = os.path.join(root, filename)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if "votes" not in data:
                print(f"Skipped (no 'votes' key): {file_path}")
                continue

            votes = data["votes"]

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(votes, f, indent=4, ensure_ascii=False)

            print(f"Processed: {file_path}")

        except json.JSONDecodeError:
            print(f"Skipped (invalid JSON): {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")