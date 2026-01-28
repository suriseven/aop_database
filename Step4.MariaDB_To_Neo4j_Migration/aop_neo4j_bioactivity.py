import json
import re
import pandas as pd
from neo4j import GraphDatabase
import mysql.connector

with open('mariadb_dbinfo' , 'r') as f:
    madiadb_info = json.load(f)

mariadb = mysql.connector.connect(
    host=madiadb_info["host"],
    port=madiadb_info["port"],
    user=madiadb_info["username"],
    password=madiadb_info["password"],
    database=madiadb_info["database"]
)
cursor = mariadb.cursor(dictionary=True, buffered=False)

excel_folder = "/home/ubuntu/toxcast_rawdata/bioactivity" 
with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

def clean_props(record):
    return {
        re.sub(r'\W+', '_', k): (str(v) if pd.notnull(v) else None)
        for k, v in record.items()
    }

def parse_ids(cell_value, delimiter):
    if pd.isnull(cell_value):
        return []
    # Split by delimiter(s), filter digits, convert to int
    import re
    parts = re.split(r'[| ]+', str(cell_value).strip())
    return [int(x) for x in parts if x.isdigit()]


def bulk_insert_bioactivities(tx, rows):
    tx.run("""
        UNWIND $rows AS row
        CREATE (b:Bioactivity {
            name: row.name,
            aop_ids: row.aop_ids,
            event_ids: row.event_ids,
            dtxsid: row.dtxsid
        })
        SET b += row.props
    """, rows=rows)

            
def prepare_bulk_rows(rows):
    bulk_data = []
    for row in rows:
        props = clean_props(row)
        aop_ids = parse_ids(row.get('AOP'), r'[|]')
        event_ids = parse_ids(row.get('EVENT'), r' ')
        dtxsid = row.get('DTXSID')
        bioactivity_name = row.get('NAME', 'unknown')
        id = row.get('ID', '-1')
        # unique_id = bioactivity_name + str(hash(frozenset(props.items())))

        bulk_data.append({
            "id": id,
            "name": bioactivity_name,
            "props": props,
            "aop_ids": aop_ids,
            "event_ids": event_ids,
            "dtxsid": dtxsid,
        })
    return bulk_data

chunk_size = 1000
for i in range(1, 3222):
    # --- Step 1: Query full table ---
    query = f"SELECT * FROM bioactivity where id >= {i*chunk_size + 1} and id < {(i+1)*chunk_size+1}"
    cursor.execute(query)  # Replace with your table name

    print(query)

    rows = cursor.fetchall()
    if not rows:
        continue

    print(f"[{i}] Fetched {len(rows)} bioactivity nodes")

    # Process into Neo4j-friendly format
    bulk_data = prepare_bulk_rows(rows)

    print(f"[{i}] Prepared {len(rows)} bioactivity nodes")

    # Insert in bulk
    with driver.session() as session:
        session.execute_write(bulk_insert_bioactivities, bulk_data)

    print(f"[{i}] Inserted {len(rows)} bioactivity nodes")


cursor.close()
mariadb.close()

driver.close()
