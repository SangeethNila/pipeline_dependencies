import json
import subprocess
import gitlab

from graph_creation.utils import GITLAB_ASTRON


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
    PROJECT_ID = 35  
    MERGE_REQUEST_IDS = [208, 206, 109, 175, 179, 145, 140] 

    gl = gitlab.Gitlab(GITLAB_ASTRON)
    project = gl.projects.get(PROJECT_ID)
    commit_data = []

    for id in MERGE_REQUEST_IDS:

        # Get the merge request
        merge_request = project.mergerequests.get(id)

        # Fetch commits from the merge request
        commits = merge_request.commits()
        for commit in commits:
            
            # Skip merge commits or conflict resolves
            commit_message = str(commit.message).lower()
            if "merge" in commit_message:
                if "branch" in commit_message or "conflict" in commit_message:
                    continue

            commit_details = project.commits.get(commit.id)
            commit_entry = {
                "id": commit.id,
                "changed_files": [diff['new_path'] for diff in commit_details.diff(get_all=True)]
            }
            commit_data.append(commit_entry)

    # Save to a JSON file
    with open("commits_for_evaluation.json", "w", encoding="utf-8") as file:
        json.dump(commit_data, file, indent=4)