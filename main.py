from graph_creation.repo_processing import process_repos
from neo4j import GraphDatabase
import dotenv
import os
import gitlab
import subprocess

def clone_repos(repo_list: list[str], folder_name: str):
    gl = gitlab.Gitlab('https://git.astron.nl')
    projects = gl.projects.list(iterator=True, get_all=True)
    for project in projects:
        repo_name = project.attributes['path_with_namespace']
        if repo_name in repo_list:
            git_url = project.ssh_url_to_repo
            subprocess.call(['git', 'clone', git_url, f'./{folder_name}/{repo_name}'])

if __name__ == '__main__':
    relevant_repos = ['ldv/imaging_compress_pipeline']
    folder = 'repos'
    clone_repos(relevant_repos, folder)

    load_status = dotenv.load_dotenv("Neo4j-25ebc0db-Created-2024-11-17.txt")
    if load_status is False:
        raise RuntimeError('Environment variables not loaded.')

    URI = os.getenv("NEO4J_URI")
    AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

    repo_paths = [f'{folder}/{path}' for path in relevant_repos]
    print(repo_paths)
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
        print("Connection established.")
        driver = GraphDatabase.driver(URI, auth=AUTH)
        process_repos(repo_paths, driver)
        driver.close()

