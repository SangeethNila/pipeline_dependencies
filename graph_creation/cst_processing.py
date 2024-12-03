from antlr4 import ParserRuleContext
from neo4j import Driver

from neo4j_queries.edge_queries import create_has_child_relationship
from neo4j_queries.node_queries import create_ast_node

def traverse_and_create(driver: Driver, tree, parent_node_id=None):
    # Create a Neo4j node for the current tree node
    rule_name = type(tree).__name__
    text = tree.getText() if tree.getText() else None

    current_node_id = create_ast_node(driver, rule_name, text)

    # If there's a parent, create a relationship
    if parent_node_id is not None:
        create_has_child_relationship(driver, parent_node_id, current_node_id)

    # Recursively process all children
    for i in range(tree.getChildCount()):
        child = tree.getChild(i)
        traverse_and_create(driver, child, current_node_id)


def traverse_when_statement_extract_dependencies(tree: ParserRuleContext) -> list[tuple[str,str]]:
    """
    This function traverses a ParserRuleContext tree of a JS expression created by ANTLR
    to extract dependencies specified in a "when" statement of a CWL workflow step. 
    Dependencies include references to step input parameter and outputs of other steps, 
    which are identified and categorized during the traversal.

    Parameters:
        tree (ParserRuleContext): the tree obtained by parsing a JS expression statement
        
    Returns:
        list[tuple[str,str]]: a list of references to inputs or outputs. Each reference is a tuple.
            The first element of the tuple is either "parameter" or "step_output", the second parameter is the ID of the element.
            In the case of the step output, the ID is [workflow-level step ID]/[output ID]
    """
    rule_name = type(tree).__name__
    text = tree.getText() if tree.getText() else None
    ref_list = []

    # The "when" field of a step can reference:
    # - inputs (parameters) of that step in the form input.[param ID]
    # - outputs of different steps in the form steps.[step ID].outputs.[output ID]
    if rule_name == "MemberDotExpressionContext":
        split_text = text.split(".")
        if len(split_text) == 2:
            if split_text[0] == "inputs":
                ref_list.append(("parameter", split_text[1])) 
        elif len(split_text) == 4:
            if split_text[0] == "steps" and split_text[2] == "outputs":
                ref_list.append(("step_output", split_text[1] + "/" + split_text[3]))

    # Recursively process all children
    for i in range(tree.getChildCount()):
        child = tree.getChild(i)
        ref_list_child = traverse_when_statement_extract_dependencies(child)
        ref_list.extend(ref_list_child)
    
    return ref_list
