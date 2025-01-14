from pathlib import Path
import ruamel.yaml
import chardet

from neo4j_queries.utils import get_is_workflow


def get_cwl_from_repo(repo_path: str) -> tuple[list[dict],list[dict]]:
    """
    Processes all CWL (Common Workflow Language) files in a given repository, categorizing them into workflows and tools.

    Parameters:
        repo_path (str): The path to the local repository containing CWL files.

    Returns:
        tuple[list[dict], list[dict]]: 
            - The first list contains dictionaries representing parsed CWL workflow files.
            - The second list contains dictionaries representing parsed CWL tool files.
    """
    cwl_workflow_entities = []
    cwl_tool_entities = []
    # Recursively find all CWL files in the repository
    pathlist = Path(repo_path).glob('**/*.cwl')

    for path in pathlist:
        path_in_str = str(path)

        # Detect file encoding to handle non-UTF-8 encoded files
        with open(path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']

        # Open the file using the detected encoding and parse it as YAML
        with open(path, "r", encoding=encoding) as file:
            yaml = ruamel.yaml.YAML()
            yaml_dict = yaml.load(file)

            # Add the file path to the dictionary for reference
            yaml_dict['path'] = path_in_str

            # Categorize the file based on its 'class' field
            if get_is_workflow(yaml_dict):
                cwl_workflow_entities.append(yaml_dict)
            else:
                cwl_tool_entities.append(yaml_dict)

    return cwl_workflow_entities, cwl_tool_entities