from pathlib import Path
from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile

def get_cwl_from_repo(repo_path: str) -> list[dict]:
    """
    Given the path of a local repository, it processes all the CWL files in the repository.
    Each CWL file is parsed into a dictionary using the cwl_utils library.
    The path is saved using the key 'path' with value equal to the relative path of the CWL file.

    Parameters:
        repo_path (str): the path of the local repository

    Returns:
        list[dict]: a list of dictonaries, each dictionary is a parsed CWL file
    """
    cwl_entities = []
    pathlist = Path(repo_path).glob('**/*.cwl')
    for path in pathlist:
        path_in_str = str(path)
        # Parse CWL file
        cwl_obj = load_inputfile(path_in_str)
        # Save parsed file into a dictionary
        saved_obj = save(cwl_obj,  relative_uris=True)
        # Save the path of the CWL file
        saved_obj['path'] = path_in_str
        cwl_entities.append(saved_obj)

    return cwl_entities