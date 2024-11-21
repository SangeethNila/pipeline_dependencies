from pathlib import Path
from cwl_utils.parser import save
from cwl_utils.parser.cwl_v1_2_utils import load_inputfile

def get_cwl_from_repo(repo_path: str) -> list[dict]:
    cwl_entities = []
    pathlist = Path(repo_path).glob('**/*.cwl')
    for path in pathlist:
        path_in_str = str(path)   
        cwl_obj = load_inputfile(path_in_str)
        saved_obj = save(cwl_obj,  relative_uris=True)
        saved_obj['path'] = path_in_str
        cwl_entities.append(saved_obj)

    return cwl_entities