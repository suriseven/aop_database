import re
import os
import requests
import warnings
import json
import pandas as pd
from neo4j import GraphDatabase
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

# === Neo4j connection config ===
with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

NEO4J_URI = neo4j_info["uri"]
NEO4J_USER = neo4j_info["username"]
NEO4J_PASSWORD = neo4j_info["password"]


driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

cypher_queries = {
    "AOP":      "MATCH (n:AOP) RETURN max(toInteger(replace(toString(n.id), 'AOP', ''))) AS max_id",
    "Event":    "MATCH (n:Event) RETURN max(toInteger(replace(toString(n.id), 'KE', ''))) AS max_id",
    "Stressor": "MATCH (n:Stressor) RETURN max(toInteger(replace(toString(n.id), 'STR', ''))) AS max_id",
    "KER":      "MATCH (n:KE_Relation) RETURN max(toInteger(replace(toString(n.id), 'KER', ''))) AS max_id"
}


# === File paths for each type ===
csv_files = {
    "AOP": "aops.csv",
    "Event": "events.csv",
    "Stressor": "stressors.csv",
    "KER": "ke-relationships.csv"
}

# === ID prefixes for each type ===
prefixes = {
    "AOP": "AOP",
    "Event": "KE",
    "Stressor": "STR",
    "KER": "KER"
}

base_url = 'https://aopwiki.org/aops'
save_dir = 'temp'

def download_html(label, id):    
    url = f'{base_url}/{id}'
    download_dir = os.path.join(save_dir, f'{label}')    
    download_path = os.path.join(download_dir, f'{id}.html')

    response = requests.get(url, verify=False)
    
    if response.status_code == 200: 
        if 'This event does not exist.' not in response.text:      
            os.makedirs(download_dir, exist_ok=True)
            with open(download_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print("Page downloaded successfully.")
        else:
            print(f'event id {node_id} does not exist')
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")


# === Helper: run a Cypher query and return the max ID
def get_max_id(label):
    query = cypher_queries[label]
    with driver.session() as session:
        result = session.run(query)
        record = result.single()
        return record["max_id"] or 0

def run_query(tx, query, params=None):
    tx.run(query, params or {})   

def insert_aop(aops_df):
    with driver.session() as session:
        ## === 1. AOPs ===
        for _, row in aops_df.iterrows():
            session.write_transaction(run_query, """
            MERGE (a:AOP {id: $id})
            SET a.title = $title,
                a.contact = $contact,
                a.license = $license,
                a.mie = $mie,
                a.ao = $ao,
                a.status = $status,
                a.project = $project
            """, {
                "id": int(row.ID),
                "title": row.Title,
                "contact": row["Point of Contact"],
                "license": row.License,
                "mie": row.MIE,
                "ao": row.AO,
                "status": row["OECD Status"],
                "project": row["OECD Project"]
            })

def insert_event(events_df):
    with driver.session() as session:
        ## === 2. Events ===
        for _, row in events_df.iterrows():
            session.write_transaction(run_query, """
            MERGE (e:Event {id: $id})
            SET e.title = $title,
                e.biological_organization = $biological_organization,
                e.creation_date = $creation_date,
                e.last_updated = $last_updated,
                e.AOPs = $aops     
            """, {
                "id": int(row.ID),
                "title": row.Title,
                "biological_organization": row["Biological organization"],
                "creation_date": row["Creation Date"],
                "last_updated": row["Last Updated"],
                "aops": row["AOPs"],
            })

            if pd.notna(row.AOPs):
                session.write_transaction(run_query, """
                MATCH (a:AOP), (e:Event {id: $eid})
                WHERE $aop_title CONTAINS a.title OR a.title CONTAINS $aop_title
                MERGE (a)-[:HAS_EVENT]->(e)
                """, {
                    "eid": int(row.ID),
                    "aop_title": row.AOPs
                })

def insert_kerlation(ke_df):
    with driver.session() as session:
        ## === 3. KE Relationships (KE_Relation) ===
        for _, row in ke_df.iterrows():
            rel_id = int(row.ID)
            upstream = row["Upstream Event"]
            downstream = row["Downstream Event"]
            creation_date = row["Creation Date"]
            last_updated = row["Last Updated"]
            aops = row["AOPs"]

            session.write_transaction(run_query, """
            MERGE (up:Event {title: $up})
            MERGE (down:Event {title: $down})
            MERGE (rel:KE_Relation {id: $id})
            MERGE (rel)-[:CAUSES]->(down)
            MERGE (up)-[:CAUSES]->(rel)
            SET rel.upstream_event = $up,
                rel.downstream_event = $down,
                rel.last_updated = $last_updated,
                rel.creation_date = $creation_date,
                rel.AOPs = $aops
            """, {
                "id": rel_id,
                "up": upstream,
                "down": downstream,
                "creation_date": creation_date,
                "last_updated": last_updated,
                "aops" : aops
            })

            if pd.notna(row.AOPs):
                session.write_transaction(run_query, """
                MATCH (a:AOP), (rel:KE_Relation {id: $id})
                WHERE $aop_title CONTAINS a.title OR a.title CONTAINS $aop_title
                MERGE (a)-[:HAS_RELATIONSHIP]->(rel)
                """, {
                    "id": rel_id,
                    "aop_title": row.AOPs
                })

def insert_stressor(stressors_df):
    with driver.session() as session:
        ## === 4. Stressors ===
        for _, row in stressors_df.iterrows():
            sid = int(row.ID)
            aop_id = row["Associated AOPs"]

            # Create the stressor node with aop_id as property
            session.write_transaction(run_query, """
            MERGE (s:Stressor {id: $id})
            SET s.name = $name,
                s.chemical = $chem,
                s.aop_id = $aop_id
            """, {
                "id": sid,
                "name": row["Prototypical Stressor Name"],
                "chem": row["Associated Chemical(s): Name, CAS-RN, DTXSID"],
                "aop_id": aop_id
            })

            # Link directly using aop_id = AOP.id
            if pd.notna(aop_id) and str(aop_id).isdigit():
                session.write_transaction(run_query, """
                MATCH (a:AOP {id: toInteger($aop_id)}), (s:Stressor {id: $sid})
                MERGE (s)-[:ASSOCIATED_WITH]->(a)
                """, {
                    "aop_id": int(aop_id),
                    "sid": sid
                })

def clean_text(text):
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()

def html_to_text_with_csv_tables(html):
    soup = BeautifulSoup(html, 'html.parser')
    main = soup.find('main') or soup

    text_lines = []
    table_count = 0

    for elem in main.descendants:
        if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
            text_lines.append('\n' + elem.get_text(strip=True).upper() + '\n' + ('=' * 40))
        elif elem.name == 'p':
            ptext = elem.get_text(strip=True)
            if ptext:
                text_lines.append(ptext)
        elif elem.name == 'table':
            for tr in elem.find_all('tr'):
                row = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
                text_lines.append(','.join(row))

    return clean_text('\n\n'.join(text_lines))

def get_nodes(tx, label):
    query = f"MATCH (n:{label}) RETURN n.id AS id"
    result = tx.run(query)
    return [record["id"] for record in result]

def update_node_text(label, node_id, text):
    with driver.session() as session:
        session.write_transaction(run_query, 
            f"""
            MATCH (n:{label} {{id: $id}})
            SET n.text_content = $text
            """,{
                'id':node_id,
                'text':text})

def load_html_file(label, node_id):
    filepath = f"{save_dir}/{label}/{node_id}.html"
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return html_to_text_with_csv_tables(f.read())
    except FileNotFoundError:
        print(f"[WARN] File not found: {filepath}")
        return None

# === Main processing loop ===
for label, func in [("AOP", insert_aop), ("Event", insert_event), ("Stressor", insert_stressor), ("KER", insert_kerlation)]:    
    max_id = get_max_id(label)
    prefix = prefixes[label]
    filepath = csv_files[label]

    # Load and parse CSV
    df = pd.read_csv(filepath, dtype=str)
    df["numeric_id"] = df["ID"].str.replace(prefix, "", regex=False).astype(int)

    # Filter new entries
    new_entries = df[df["numeric_id"] > max_id]

    print(f"\nNew {label} entries (ID > {max_id}): {len(new_entries)} found")
    print(new_entries[["ID"]])

    new_entries.iterrows()

    func(new_entries)

    for node_id in new_entries["ID"].to_list():
        download_html(label, node_id)

        html_text = load_html_file(label, node_id)
        if html_text:
            update_node_text(label, node_id, html_text)
            print(f"Updated node {label} {node_id}")
        else:
            print(f"Skipped node {label} {node_id} (no html)")
    

driver.close()
