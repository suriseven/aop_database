import json
import re
import pandas as pd
from neo4j import GraphDatabase
import mysql.connector

# --- Connect to MariaDB ---
import json

with open('mariadb_dbinfo' , 'r') as f:
    db_info = json.load(f)

mariadb = mysql.connector.connect(
    host=db_info["host"],
    port=db_info["port"],
    user=db_info["username"],
    password=db_info["password"],
    database=db_info["database"]
)
cursor = mariadb.cursor(dictionary=True, buffered=False)

# === CONFIG ===
excel_folder = "/home/ubuntu/toxcast_rawdata/bioactivity" 
with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

# === Neo4j Driver Setup ===
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


def create_bioactivity_node(tx, name, props, aop_ids, event_ids, dtxsid):
    # print(aop_ids, event_ids)
    tx.run("""
        MERGE (b:Bioactivity {unique_id: $unique_id})
        SET b.name = $name
        SET b += $props
    """, unique_id=name + str(hash(frozenset(props.items()))), 
         name=name, props=props, aop_ids=aop_ids, event_ids=event_ids)

def bulk_insert_bioactivities(tx, rows):
    tx.run("""
        UNWIND $rows AS row
        MERGE (b:Bioactivity {unique_id: row.unique_id})
        SET b.name = row.name
        SET b += row.props
        SET b.aop_ids = row.aop_ids
        SET b.event_ids = row.event_ids
        SET b.dtxsid = row.dtxsid
    """, rows=rows)

def process_excel(file_path):
    filename = file_path.split('/')[-1]
    bioactivity_name = filename.split(" Toxcast Summary")[0].strip()
    
    df = pd.read_excel(file_path)

    with driver.session() as session:
        for idx, row in df.iterrows():
            props = clean_props(row.to_dict())
            aop_ids = parse_ids(row.get('AOP', ''), '|')
            event_ids = parse_ids(row.get('EVENT', ''), ' ')
            session.execute_write(create_bioactivity_node, bioactivity_name, props, aop_ids, event_ids)
            print(f"Created Bioactivity node for row {idx + 1}")
            
def prepare_bulk_rows(rows):
    bulk_data = []
    for row in rows:
        props = clean_props(row)
        aop_ids = parse_ids(row.get('AOP'), r'[|]')
        event_ids = parse_ids(row.get('EVENT'), r' ')
        dtxsid = row.get('DTXSID')
        bioactivity_name = row.get('NAME', 'unknown')
        unique_id = bioactivity_name + str(hash(frozenset(props.items())))

        bulk_data.append({
            "unique_id": unique_id,
            "name": bioactivity_name,
            "props": props,
            "aop_ids": aop_ids,
            "event_ids": event_ids,
            "dtxsid": dtxsid,
        })
    return bulk_data

chunk_size = 1000
for i in range(331, 2750):
    # --- Step 1: Query full table ---
    query = f"SELECT * FROM bioactivity where id >= {i*chunk_size + 1} and id < {(i+1)*chunk_size+1} and HIT_CALL = 'Inactive' "
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

    # --- Step 2: Send all rows to Neo4j ---
    # with driver.session() as session:
    #     for row in cursor:
    #         props = clean_props(row)
    #         aop_ids = parse_ids(row.get('AOP'), r'[|]')
    #         event_ids = parse_ids(row.get('EVENT'), r' ')
    #         dtxsid = row.get('DTXSID')
    #         bioactivity_name = row.get('NAME', 'unknown')

    #         session.write_transaction(create_bioactivity_node, bioactivity_name, props, aop_ids, event_ids, dtxsid)
    #         print(f"[{i}] Created Bioactivity node for {bioactivity_name} with DTXSID {dtxsid}")

cursor.close()
mariadb.close()

driver.close()
