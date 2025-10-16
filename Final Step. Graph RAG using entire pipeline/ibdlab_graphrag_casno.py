import json
import threading
import time
from neo4j import GraphDatabase
from neo4j_graphrag.retrievers.base import Retriever
from neo4j_graphrag.types import RetrieverResult, RetrieverResultItem
from neo4j_graphrag.generation.graphrag import GraphRAG
from neo4j_graphrag.llm import OllamaLLM

with open('neo4j_dbinfo', 'r') as f:
    neo4j_info = json.load(f)

neo4j_uri = neo4j_info["uri"]
neo4j_user = neo4j_info["username"]
neo4j_password = neo4j_info["password"]

llm = OllamaLLM("llama3.1:8b")

def extract_casrn(question: str):
    llm_prompt = f"""
    주어진 문장으로부터 화학물질의 CAS-NO를 찾으세요
    \"{question}\"
    당신의 답변은 숫자와 hyphen기호로만 이루어진 CAS-NO 뿐입니다
    
    [주의사항]
    다른 문장이나 접두사 접미사 조사등 절대, Never, Anything 섞지 않습니다
    """

    casrn = llm.invoke(llm_prompt)
    
    print(f'문장에 포함된 CAS-NO: {casrn.content}')
    
    return str(casrn.content)



class KeywordCypherRetriever(Retriever):
    def __init__(self, uri, user, password, start_label="Document"):
        driver = GraphDatabase.driver(uri, auth=(user, password))
        super().__init__(driver) 
        self.start_label = start_label

    def search(self, query_text: str, **kwargs) -> RetrieverResult:
        casrn = extract_casrn(query_text)
        if not casrn:
            return RetrieverResult(items=[])

        cypher = """
        MATCH (p:Product)-[pe]-(c:Chemical)-[ce]-(b:Bioactivity)-[be]-(e:Event)
        WHERE c.casrn = $casrn
        WITH p, pe, c, ce, b, be, e
        MATCH (disease:CTD_Chem_Disease)-[de]-(c:Chemical)-[ge]-(go:CTD_Chem_GO)
        OPTIONAL MATCH path=(e)-[ee:LEADS_TO*1..3]->(e_chain:Event)
        with p, c, b, e, go
        LIMIT 5000
        RETURN p, collect(DISTINCT go) as GO, collect(DISTINCT c) AS chemicals, collect(DISTINCT b) AS bioacts, collect(DISTINCT e) AS events
        """

        rows = self.driver.session().run(cypher, casrn=casrn).data()
        # print(rows)

        print(f"IBDLAB GRAPH DB로부터 검색된 화학제품: {len(rows)}")
        for row in rows:
            p = row["p"]
            # print("Product name:", p.get("category"))
            # print("Description:", p.get("emergency"))

        items = []
        for row in rows:
            category = row["p"].get("category")
            emergency = row["p"].get("emergency")
            chems = [c.get("name") for c in row["chemicals"] if c]
            bioacts = [b.get("ASSAY_DESC") for b in row["bioacts"] if b]
            events = [e.get("title") for e in row["events"] if e]
            events_detail = [e.get("text_content") for e in row["events"] if e]
            go_term_ids = [g.get("GOTermID") for g in row["GO"] if g]
            go_term_names = [g.get("GOTermName") for g in row["GO"] if g]

            text_parts = [f"Product name: {p.get('product_name', '')}"]
            if category:
                text_parts.append(f"Its category is: {category}")
            if emergency:
                text_parts.append(f"In case of emergency from this: {emergency}")
            if chems:
                text_parts.append(f"Contains chemicals: {', '.join(chems)}")
            if bioacts:
                text_parts.append(f"Linked bioactivities: {', '.join(bioacts)}")
            if events:
                text_parts.append(f"Associated AOP events: {', '.join(events)}")
            if go_term_ids:
                text_parts.append(f"Affected GOTermID: {', '.join(go_term_ids)}")
                text_parts.append(f"Affected GOTerm Name: {', '.join(go_term_names)}")
            # if events_detail:
            #     text_parts.append(f"The details about AOP events: {', '.join(events_detail)}")

            text = ". ".join(text_parts)
            # print(text)
            items.append(RetrieverResultItem(content=text, metadata={"product": p}))

        return RetrieverResult(items=items)
    
    from neo4j_graphrag.generation.graphrag import GraphRAG
from neo4j_graphrag.llm import OllamaLLM

retriever = KeywordCypherRetriever(
    neo4j_uri,
    neo4j_user,
    neo4j_password,
    start_label="Product"
)

rag = GraphRAG(retriever=retriever, llm=llm)

def show_loading():
    while not done[0]:
        for c in ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]:
            print(f"\r열심히 찾는중... {c}", end=" ", flush=True)
            time.sleep(0.1)

done = [False]

while True:
    user_prompt = input("\033[32m[Chat IBDLAB] 궁금한 것이 있나요?\033[0m\n")
    
    done[0] = False
    t = threading.Thread(target=show_loading)
    t.start()
    
    response = rag.search(query_text=user_prompt)
    
    done[0] = True
    t.join()
    
    print(f"\r{' ' * 30}\r")
    print("\n\033[34m답변은 아래와 같습니다\033[0m\n", response.answer, "\n")
