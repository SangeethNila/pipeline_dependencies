import git
from pathlib import Path
from collections import defaultdict
import pandas as pd

from graph_creation.utils import resolve_relative_path
from neo4j_queries.utils import clean_component_id

def get_cwl_change_history(repo_path):
    """
    Get the commit history of a GitLab repository and track co-occurrences of CWL file changes.
    
    :param repo_path: Path to the local Git repository
    :return: Dictionary of CWL file co-occurrence counts
    """
    # Open the repository
    repo = git.Repo(repo_path)
    
    # Dictionary to store co-occurrence counts
    cwl_changes = defaultdict(lambda: defaultdict(int))
    
    # Get commit history
    for commit in repo.iter_commits():
        # Get list of changed files in the commit
        changed_files = [clean_component_id(str(resolve_relative_path(Path(repo_path) / file.a_path))) for file in commit.diff(None)]
        
        # Filter only .cwl files
        cwl_files = [file for file in changed_files if file.endswith(".cwl")]
        
        # Count co-occurrences
        for file in cwl_files:
            cwl_changes[file]['total'] += 1
            for other_file in cwl_files:
                if file != other_file:
                    cwl_changes[file][other_file] += 1
    
    # Convert to a normal dictionary for output
    return {k: dict(v) for k, v in cwl_changes.items()}

def get_fan_sets(data):
    fan_dict = {}
    for entity in data:
        component_id = entity['component_id']
        fan_dict[component_id] = {}
        fan_dict[component_id]['fan_in_out_set'] = set(entity['fan_in_set']).union(set(entity['fan_out_set']))
        fan_dict[component_id]['fan_in_set'] = entity['fan_in_set']
        fan_dict[component_id]['fan_out_set'] = entity['fan_out_set']

    return fan_dict

def save_history_ratios(repo_paths, data):
    all_component_ids = [entity['component_id'] for entity in data]
    # Create the matrix dataframe
    matrix = pd.DataFrame(-1, index=all_component_ids, columns=all_component_ids)

    for repo in repo_paths:
        history_dict = get_cwl_change_history(repo)
        repo_id = clean_component_id(str(Path(repo)))
        filtered_data = [d for d in data if repo_id in d['component_id']]

        for entity_1 in filtered_data:
            component_id_1 = entity_1['component_id']
            if component_id_1 in history_dict:
                print(f"Analyzing history of {component_id_1}")
                entity_1_changes = history_dict[component_id_1]
                for entity_2 in filtered_data:
                    component_id_2 = entity_2['component_id']
                    if component_id_2 in entity_1_changes:
                        ratio = entity_1_changes[component_id_2] / history_dict[component_id_1]['total']
                        matrix.at[component_id_1, component_id_2] = ratio
                    else:
                        matrix.at[component_id_1, component_id_2] = 0

    matrix.to_csv("history_ratios.csv", index=True)