from pathlib import Path
import ruamel.yaml
import chardet

def get_cwl_from_repo(repo_path: str) -> list[dict]:
    """
    Processes all CWL (Common Workflow Language) files in a given repository.

    Parameters:
        repo_path (str): The path to the local repository containing CWL files.

    Returns:
        list[dict]: 
            list of dictionaries representing parsed CWL files.
    """
    cwl_entities = []
    # Recursively find all CWL files in the repository
    pathlist = list(Path(repo_path).rglob("*.cwl"))

    for path in pathlist:
        processed_cwl = process_cwl_file(str(path))
        cwl_entities.append(processed_cwl)
    return cwl_entities

def process_cwl_file(path: str) -> dict:
    """
    Processes a Common Workflow Language (CWL) file by detecting its encoding 
    and parsing it as YAML.

    Parameters:
        path (str): The file path to the CWL file.

    Returns:
        dict: A dictionary representation of the YAML content, with an additional 
              'path' key containing the file path.
    
    Notes:
        - Uses `chardet` to detect file encoding, ensuring compatibility with 
          non-UTF-8 encoded files.
        - Uses `ruamel.yaml` for YAML parsing to preserve formatting and ordering.
    """
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
        yaml_dict['path'] = path

        return yaml_dict