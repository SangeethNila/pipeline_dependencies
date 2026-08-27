
def delete_graph(driver):
    with driver.session() as session:
            # Delete all nodes and relationships
        session.run("MATCH (n) DETACH DELETE n")

        # Drop all constraints
        constraints = session.run("SHOW CONSTRAINTS")
        for record in constraints:
            name = record["name"]
            session.run(f"DROP CONSTRAINT {name}")

        # Drop all indexes
        indexes = session.run("SHOW INDEXES")
        for record in indexes:
            name = record["name"]
            session.run(f"DROP INDEX {name}")

        print("Neo4j database cleaned successfully.")
