import os
import json
import pandas as pd
from neo4j import GraphDatabase
import re

assay_base_dir = '/home/ubuntu/toxcast_rawdata/assays'

with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))


def parse_assay_name(filename):
    pattern = r"Assay List (.+)-\d{4}-\d{2}-\d{2}\.xlsx"
    match = re.match(pattern, filename)
    if match:
        return match.group(1)

    return None


def create_assay_batch(tx, rows):
    query = """
    UNWIND $rows AS row

    CREATE (a:Assay)
    SET a = row
    """

    tx.run(query, rows=rows)


with driver.session() as session:
    for root, dirs, files in os.walk(assay_base_dir):
        for f in files:
            if f.endswith(".xlsx"):
                file_path = os.path.join(root, f)
                assay_name = parse_assay_name(f)

                if assay_name is None:
                    print(f"Skip : {f}")
                    continue

                print(f"Loading : {assay_name}")

                df = pd.read_excel(file_path)

                rows = []

                for _, row in df.iterrows():

                    props = {
                        "name": assay_name,

                        "dtxsid": row["DTXSID"],
                        "preferred_name": row["PREFERRED NAME"],
                        "casrn": row["CASRN"],
                        "molecular_formula": row["MOLECULAR FORMULA"],
                        "monoisotopic_mass": row["MONOISOTOPIC MASS"],

                        "toxcast_active": row["ToxCast Active"],
                        "toxcast_total": row["ToxCast Total"],
                        "toxcast_active_percent": row["% ToxCast Active"],

                        "hit_call": row["HIT CALL"],

                        "top": row["TOP"],
                        "scaled_top": row["SCALED TOP"],

                        "ac50": row["AC50"],
                        "logac50": row["LOGAC50"]
                    }

                    rows.append(props)
                    # print(props)

                session.execute_write(
                    create_assay_batch,
                    rows
                )

                print(f"Done : {assay_name}")


driver.close()


# Post processing to build relations
#
# CALL apoc.periodic.iterate(
# '
# MATCH (c:Chemical)
# RETURN c
# ',
# '
# MATCH (a:Assay {dtxsid:c.dtxsid})
# MERGE (c)-[:HAS_ASSAY_RESULT]->(a)
# ',
# {
#     batchSize:10,
#     parallel:false
# }
# );

# CALL apoc.periodic.iterate(
# '
# MATCH (c:cHTSMT)
# RETURN c
# ',
# '
# MATCH (a:Assay {name:c.assay})
# MERGE (c)-[:HAS_ASSAY]->(a)
# ',
# {
#     batchSize:10,
#     parallel:false
# }
# );
