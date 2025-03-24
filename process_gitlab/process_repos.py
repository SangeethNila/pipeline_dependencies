import json
import os
from pathlib import Path
import subprocess
import gitlab

from graph_creation.utils import GITLAB_ASTRON
from process_gitlab.utils import CONSIDERED_MRS, GIT_ID_TO_NAME, SHORTHAND_TO_PATH


def clone_repos(repo_list: list[str], folder_name: str) -> None:
    """
    Given a list of relative paths to ASTRON GitLab repositories and the name of a folder,
    the mentioned repositories are cloned into the mentioned folder.

    Parameters:
        repo_list (list[str]): list of relative paths to ASTRON GitLab repositories
        folder_name (str): the name of the folder to clone the repos into
    """
    gl = gitlab.Gitlab(GITLAB_ASTRON)
    projects = gl.projects.list(iterator=True, get_all=True)
    for project in projects:
        repo_name = project.attributes['path_with_namespace']
        if repo_name in repo_list:
            git_url = project.ssh_url_to_repo
            subprocess.call(['git', 'clone', git_url, f'./{folder_name}/{repo_name}'])

def save_commit_history_for_evaluation():

    # Ensure the directory exists
    directory = "commit_data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    gl = gitlab.Gitlab(GITLAB_ASTRON)

    for proj_id, reqs in CONSIDERED_MRS.items():
        project = gl.projects.get(proj_id)
        commit_data = []

        print(f"Project {proj_id}")

        for req_id in reqs:

            # Get the merge request
            merge_request = project.mergerequests.get(req_id)

            # Fetch commits from the merge request
            commits = merge_request.commits()
            for commit in commits:
                
                # Skip merge commits or conflict resolves
                commit_message = str(commit.message).lower()
                if "merge" in commit_message:
                    if "branch" in commit_message or "conflict" in commit_message:
                        continue

                if "doc" in commit_message or "comment" in commit_message:
                    continue

                commit_details = project.commits.get(commit.id)
                diffs = commit_details.diff(get_all=True)
                repo_name = GIT_ID_TO_NAME[proj_id]
                repo_path = SHORTHAND_TO_PATH[repo_name]
                changed_cwl = [repo_path + str(Path(diff['new_path'])) for diff in diffs if diff['new_path'].endswith('.cwl')]

                commit_entry = {
                    "id": commit.id,
                    "changed_files": changed_cwl
                }

                if len(changed_cwl) > 1:
                    commit_data.append(commit_entry)

        commit_count = len(commit_data)
        print(f"final commit count {commit_count}")

        if commit_count >= 30:
            # Save to a JSON file
            with open(f"commit_data/{repo_name}_commits_for_evaluation.json", "w", encoding="utf-8") as file:
                json.dump(commit_data, file, indent=4)