def clean_component_id(prefixed_component_id: str) -> str:
    component_id = prefixed_component_id.removeprefix("repos\\")
    return component_id