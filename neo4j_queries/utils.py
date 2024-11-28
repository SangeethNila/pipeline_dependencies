def clean_component_id(prefixed_component_id: str) -> str:
    """
    Cleans the local folder name (repos) from the repository path.

    Parameters:
    prefixed_component_id (str): the local relative path of a file in a repository located in the "repos" folder
    
    Returns:
    str: the cleaned relative path of a file
    """
    component_id = prefixed_component_id.removeprefix("repos\\")
    return component_id