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

def get_is_workflow(cwl_entity: dict) -> bool:
    """
    Determines if a given CWL entity represents a workflow.

    Parameters:
        cwl_entity (dict): A dictionary representing a CWL entity, which includes a 'class' key.

    Returns:
        bool: True if the CWL entity is a workflow, False otherwise.
    """
    return get_is_workflow_class(cwl_entity['class'])

def get_is_workflow_class(class_name: str) -> bool:
    """
    Determines if a given string is the name of the CWL class representing a workflow.

    Parameters:
        class_name (dict): A string representing the name of a CWL class.

    Returns:
        bool: True if the class represents a workflow, False otherwise.
    """
    return class_name == 'Workflow'