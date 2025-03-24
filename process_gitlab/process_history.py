import json
from pathlib import Path
from collections import defaultdict
import pandas as pd

from process_gitlab.utils import EVAL_REPOS, SHORTHAND_TO_PATH

def calculate_all_co_change_perc():
    for repo in EVAL_REPOS:
        with open(f"commit_data/{repo}_commits_for_evaluation.json", "r") as json_file:
            commit_history = json.load(json_file)
        calculate_co_change_perc(commit_history, repo)

def calculate_co_change_perc(commit_history: list[dict], repo):

    # Dictionary to store the co-change counts
    co_change_counts = defaultdict(int)
    file_change_counts = defaultdict(int)


    # Iterate through each commit in the repository
    for commit in commit_history:
        files: list[str] = commit["changed_files"]

        cwl_files = {file for file in files if file.endswith('cwl')}

        if len(cwl_files) < 2:
            continue

        # Update change counts for each individual file
        for file in cwl_files:
            file_change_counts[file] += 1

        # Update co-change counts for each pair of files modified together
        for file1 in cwl_files:
            for file2 in cwl_files:
                if file1 >= file2:
                    pair = (file1, file2)
                    co_change_counts[pair] += 1

    # Calculate the co-change ratio for each pair of files
    files = sorted(file_change_counts.keys())
    co_change_ratios = pd.DataFrame(index=files, columns=files, data=0.0)

    for file in files:
        co_change_ratios.at[file2, file1] = 100.0

    for pair, co_count in co_change_counts.items():
        file1, file2 = pair
        total_changes = file_change_counts[file1] + file_change_counts[file2] - co_count

        if total_changes > 0:
            ratio = (co_count / total_changes) * 100

        else:
            ratio = 0

        co_change_ratios.at[file1, file2] = ratio
        co_change_ratios.at[file2, file1] = ratio

    co_change_ratios.to_csv(f"commit_data/co-change_percentages/{repo}_history_percent.csv")

    return co_change_ratios
