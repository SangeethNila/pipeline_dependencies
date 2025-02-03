from pathlib import Path, PurePath
import dockerfile
import gitlab

from graph_creation.utils import GITLAB_ASTRON

def parse_all_dockerfiles(repo_path):
    pathlist = Path(repo_path).glob('**/Dockerfile*')
    all_commands = []
    for path in pathlist:
        commands = parse_dockerfile_run_commands(path)
        all_commands.extend(commands)
    mentioned_repos = handle_git_clone_commands(all_commands)
    moves = handle_ln_commands(all_commands)
    links = {"paths": {}, "commands": {}}
    link_paths = links["paths"]
    link_commands = links["commands"]
    for repo_url in mentioned_repos["no-checkout"]:
        repo_name = get_repo_name(repo_url)
        link_commands[repo_name] = repo_url
        # print(f"Repository URL with --no-checkout: {repo_url}. Alias: {repo_name}")
    for (repo_url, repo_folder) in mentioned_repos["checkout"]:
        paths = list_repo_files_from_api(repo_url)
        for path in paths:
            full_path = Path(repo_folder) / Path(path)
            link_paths[str(full_path)] = repo_url
    for move in moves:
        from_position = move[0]
        to_position = move[1]
        if from_position in link_paths:
            originating_repo = link_paths[from_position]
            link_paths[to_position] = originating_repo
        else:
            print(f"{from_position} is not a recognized path.")

    return links

def parse_dockerfile_run_commands(dockerfile_path):
    """Parse the Dockerfile and return a list of RUN commands."""
    with open(dockerfile_path, "r") as f:
        content = f.read()
        parser = dockerfile.parse_string(content)
    
    run_commands = []
    for instruction in parser:
        if instruction.cmd == "RUN":
            parts = [part.strip() for part in instruction.value[0].split('&&')]
            run_commands.extend(parts)
    return run_commands

def handle_git_clone_commands(run_commands):
    """Process RUN commands containing git clone."""
    repos = {'no-checkout': [], 'checkout': []}
    for command in run_commands:
        if "git clone" in command:
            repo_url = extract_git_repo_url(command)
            # Check if the git clone command contains --no-checkout
            if "--no-checkout" in command:
                # Extract the repository URL
                repos['no-checkout'].append(repo_url)
            else:
                # Clone the repo and list its contents
                repo_folder = extract_git_repo_folder(command)
                if repo_folder == None:
                    repo_folder = get_repo_name(repo_url)
                repos["checkout"].append((repo_url, repo_folder))
                # print(f"Repository URL without --no-checkout: {repo_url} cloned into {repo_folder}")
    return repos

def get_link_positions(ln_command):
    split_command = ln_command.split()
    for i in range(len(split_command)):
        part = split_command[i]
        if (part != "ln") and not part.startswith("-"):
            from_position = Path(part)
            to_position = Path(split_command[i+1])
            return (str(from_position), str(to_position))
        
def handle_ln_commands(run_commands):
    moves = []
    for command in run_commands:
        if command.startswith("ln"):
            move = get_link_positions(command)
            moves.append(move)
    return moves

def get_repo_name(repo_url: str) -> str:
    repo_name_with_git = PurePath(repo_url).name
    return repo_name_with_git.removesuffix('.git')

def extract_git_repo_url(command: str) -> str | None:
    """Extract the URL of the git repository from the git clone command."""
    parts = command.split()
    for part in parts:
        if is_url(part):
            return part
    return None

def extract_git_repo_folder(command: str) -> str | None:
    """Extract the folder the git repo is cloned in."""
    if "--no-checkout" not in command:
        parts = command.split()
        for i in range(len(parts)):
            part = parts[i]
            if is_url(part) and i+1 < len(parts):
                return parts[i+1]
    return None

def is_url(url: str) -> bool:
    return url.startswith("http://") or url.startswith("https://") or url.startswith("git@")

def list_repo_files_from_api(repo_url):
    """List the files in the repository using the GitHub or GitLab API."""
    if "github" in repo_url:
        repo_contents = None
    elif GITLAB_ASTRON in repo_url:
        repo_contents = list_repo_files_from_gitlab(repo_url)
    else:
        print("Unsupported repository URL")
        return
    return repo_contents

def list_repo_files_from_gitlab(repo_url):
    gl = gitlab.Gitlab(GITLAB_ASTRON)
    repo_path = repo_url.replace(GITLAB_ASTRON, "").lstrip("/").replace(":", "/").replace(".git", '')
    """List all files and directories in the repository recursively."""
    # Start listing from the root of the repository
    return list_contents_gitlab("", repo_path, gl)

def list_contents_gitlab(path, repo_path, gl):
    """List the contents of a directory and recurse into subdirectories."""
    repo_contents = fetch_gitlab_repo_contents(gl, repo_path, path)
    all_contents = []
    
    for item in repo_contents:
        all_contents.append(item['path'])
        
        # If the item is a directory, recurse into it
        if item['type'] == 'tree':
            directory_contents = list_contents_gitlab(item['path'], repo_path, gl)
            all_contents.extend(directory_contents)

    return all_contents

def fetch_gitlab_repo_contents(gl, repo_path,  path=""):
    # Get the project using the GitLab API
    project = gl.projects.get(repo_path)

    # Get the repository tree (list of files and directories)
    tree = project.repository_tree(path=path, get_all=True)
    return tree


        