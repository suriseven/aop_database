import os
import json
import pandas as pd
from neo4j import GraphDatabase

assay_base_dir = '/home/ubuntu/toxcast_rawdata/assays'

with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

def create_graph(tx, chemical, assay_name, result):
    tx.run("""
        MERGE (c:Chemical {dtxsid: $dtxsid})
        SET c.name = $name, c.casrn = $casrn, c.formula = $formula, c.mass = $mass

        MERGE (a:Assay {name: $assay_name})

        MERGE (c)-[r:HAS_RESULT {assay: $assay_name}]->(a)
        SET r.hitc = $hitc, r.ac50 = $ac50, r.logac50 = $logac50,
            r.top = $top, r.scaled_top = $scaled_top
    """, {
        "dtxsid": chemical["DTXSID"],
        "name": chemical["PREFERRED NAME"],
        "casrn": chemical["CASRN"],
        "formula": chemical["MOLECULAR FORMULA"],
        "mass": chemical["MONOISOTOPIC MASS"] if not pd.isna(chemical["MONOISOTOPIC MASS"]) else None,
        "assay_name": assay_name,
        "hitc": result["HIT CALL"] if not pd.isna(result["HIT CALL"]) else None,
        "ac50": float(result["AC50"]) if not pd.isna(result["AC50"]) else None,
        "logac50": float(result["LOGAC50"]) if not pd.isna(result["LOGAC50"]) else None,
        "top": float(result["TOP"]) if not pd.isna(result["TOP"]) else None,
        "scaled_top": float(result["SCALED TOP"]) if not pd.isna(result["SCALED TOP"]) else None,
    })


with driver.session() as session:
    for filename in os.listdir(assay_base_dir):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(assay_base_dir, filename)
            assay_name = os.path.splitext(filename)[0].split('-')[0].replace('Assay List ','')

            df = pd.read_excel(filepath)

            print(df)

            for _, row in df.iterrows():
                session.execute_write(create_graph, row, assay_name, row)
