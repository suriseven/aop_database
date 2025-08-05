import json
from neo4j import GraphDatabase

with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

with driver.session() as session:
    batch_size = 10000
    skip = 0

    while True:
        cypher = f"""
        MATCH (b:Bioactivity)
        WITH b
        ORDER BY b.id
        SKIP {skip} LIMIT {batch_size}
        UNWIND b.event_ids AS event_id
        MATCH (e:Event {{id: event_id}})
        MERGE (b)-[:RELATED_TO_EVENT]->(e)
        WITH b
        RETURN count(*) AS affected
        """

        print(cypher)

        result = session.run(cypher)
        affected = result.single()["affected"]
        print(f"Batch starting at {skip}: {affected} relationships created/merged")
        skip += batch_size

        if skip > 2760000:
            break

driver.close()
