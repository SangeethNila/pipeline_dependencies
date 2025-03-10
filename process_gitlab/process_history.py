import os
import git
from pathlib import Path
from collections import defaultdict
import pandas as pd

def calculate_co_change_ratios(commit_history: list[dict]):

    # Dictionary to store the co-change counts
    co_change_counts = defaultdict(int)
    file_change_counts = defaultdict(int)


    # Iterate through each commit in the repository
    for commit in commit_history:
        files: list[str] = commit["changed_files"]

        cwl_files = [str(Path(file))for file in files if file.endswith('cwl')]

        # Update change counts for each individual file
        for file in cwl_files:
            file_change_counts[file] += 1

        # Update co-change counts for each pair of files modified together
        for file1 in cwl_files:
            for file2 in cwl_files:
                if file1 > file2:
                    pair = (file1, file2)
                    co_change_counts[pair] += 1

    # Calculate the co-change ratio for each pair of files
    files = sorted(file_change_counts.keys())
    co_change_ratios = pd.DataFrame(index=files, columns=files, data=0.0)

    for pair, co_count in co_change_counts.items():
        file1, file2 = pair
        total_changes = file_change_counts[file1] + file_change_counts[file2] - co_count

        if total_changes > 0:
            ratio = co_count / total_changes
        else:
            ratio = 0

        co_change_ratios.at[file1, file2] = ratio
        co_change_ratios.at[file2, file1] = ratio

    co_change_ratios.to_csv(f"history_ratios.csv")

    return co_change_ratios
