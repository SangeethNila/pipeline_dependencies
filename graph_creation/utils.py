from pathlib import Path
from neo4j import Driver
from neo4j_queries.node_queries import ensure_data_node, ensure_parameter_node
from neo4j_queries.edge_queries import create_data_relationship, create_in_param_relationship

def create_input_nodes_and_relationships(driver: Driver, input_id: str, component_id: str) -> None:
    """
    Processes a single input tied to a specific CWL component. 
    The following nodes and edges are created:
    - an in-parameter node with the parameter ID as defined in the component and component ID equal to the path of the componet
    - a data node with component ID of the component and data ID equal to the parameter ID
    - a data edge from the component node to the in-parameter node
    - a data edge from the data node to the the in-parameter node

    Parameters:
        driver (Driver): the driver used to connect to Neo4j
        input_id (str): the ID of the input as defined in the CWL component
        component_id (str): the unique ID of the CWL component (its path)
    """
    # Create in-parameter with the parameter ID as defined in the component and component ID equal to the path of the componet
    param_node = ensure_parameter_node(driver, input_id, component_id, 'in')
    param_node_internal_id = param_node[0]
    # Create a data edge from the component node to the in-parameter node
    create_in_param_relationship(driver, component_id, param_node_internal_id)
    # Create a data node with component ID of the component and data ID equal to the parameter ID
    data_node = ensure_data_node(driver, input_id, component_id)
    data_node_internal_id = data_node[0]
    # Create a data edge from the data node to the the in-parameter node
    create_data_relationship(driver, data_node_internal_id, param_node_internal_id)

def process_source_relationship(driver: Driver, source_id: str, component_id: str, param_node_internal_id: int) -> None:
    """
    Processes a source relationship between a data node and a parameter node.
    The data node does not need to exist already, while the parameter node must have already been created.
    The following nodes and edges are created:
    - a data node with ID equal to source_id and component ID equal to the path of the component it belongs to
    - a data edge from the parameter node to the data node

    Parameters:
        driver (Driver): the driver used to connect to Neo4j
        source_id (str): the ID of the data that functions as a source for the parameter
        component_id (str): the unique ID of the CWL component (its path)
        param_node_internal_id (int): the unique ID of the parameter node as defined internally by Neo4j
    """
    data_node = ensure_data_node(driver, source_id, component_id)
    data_node_internal_id = data_node[0]
    create_data_relationship(driver, param_node_internal_id, data_node_internal_id)

def resolve_relative_path(path: Path)-> Path:
    """
    Resolves a relative path by simplifying `.` (current directory) 
    and `..` (parent directory) components without converting it to an absolute path.

    Parameters:
        path (Path): the input Path object to be resolved

    Returns:
        Path: a new object representing the simplified relative path

    Example:
        >>> resolve_relative_path(Path("x/y/../z"))
        Path('x/z')

        >>> resolve_relative_path(Path("./a/./b/c/../d"))
        Path('a/b/d')
    """
    parts = []
    for part in path.parts:
        if part == "..":
            if parts:
                parts.pop()
        elif part != ".":
            parts.append(part)
    return Path(*parts)