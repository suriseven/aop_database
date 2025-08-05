import os
import json
import pandas as pd
from neo4j import GraphDatabase

chemical_base_dir = '/home/ubuntu/toxcast_rawdata/chemicals'

with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

def create_chemical(tx, row):
    tx.run("""
        MERGE (c:Chemical {dtxsid: $dtxsid})
        SET c.name = $name,
            c.casrn = $casrn,
            c.inchikey = $inchikey,
            c.iupac = $iupac,
            c.smiles = $smiles,
            c.inchi = $inchi,
            c.formula = $formula,
            c.avg_mass = $avg_mass,
            c.mass = $mono_mass,
            c.qc_level = $qc,
            c.n_active = $n_active,
            c.n_total = $n_total,
            c.pct_active = $pct_active
    """, {
        "dtxsid": row["DTXSID"],
        "name": row["PREFERRED NAME"],
        "casrn": row["CASRN"],
        "inchikey": row.get("INCHIKEY", None),
        "iupac": row.get("IUPAC NAME", None),
        "smiles": row.get("SMILES", None),
        "inchi": row.get("INCHI STRING", None),
        "formula": row.get("MOLECULAR FORMULA", None),
        "avg_mass": row.get("AVERAGE MASS", None),
        "mono_mass": row.get("MONOISOTOPIC MASS", None),
        "qc": row.get("QC Level", None),
        "n_active": row.get("# ToxCast Active", None),
        "n_total": row.get("Total Assays", None),
        "pct_active": row.get("% ToxCast Active", None)
    })


with driver.session() as session:
    for filename in os.listdir(chemical_base_dir):
        if filename.endswith(".xlsx"):
            filepath = os.path.join(chemical_base_dir, filename)
            df = pd.read_excel(filepath)
            print(df)

            for _, row in df.iterrows():
                session.execute_write(create_chemical, row)
