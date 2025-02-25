from neo4j import Driver


def clean_up_flow(driver: Driver):
    with driver.session() as session:
        session.run("""
            MATCH ()-[r:DIRECT_LOCAL_FLOW]-()
            DELETE r
        """)
        session.run("""
            MATCH ()-[r:INDIRECT_LOCAL_FLOW]-()
            DELETE r
        """)

def clean_up_subgraph(driver: Driver):
    with driver.session() as session:
        session.run("""
            MATCH ()-[r:SUBGRAPH]-()
            DELETE r
        """)
