# ***AOP Database in IBD LAB, University of Seoul***
- This is the ETL pipeline code to construct AOP Database in IBD Lab, Univ. of Seoul
- We have constructed 2 database
   - MariaDB as a raw data store
   - Neo4j as a real time search engine

# ***Data sources***
- ToxCast(https://comptox.epa.gov/dashboard/)
- AOP Wiki(https://aopwiki.org/)

# ***ETL Pipeline***
### ToxCast
<img width="1071" height="476" alt="Database_construction_Fig1" src="https://github.com/user-attachments/assets/2d42b24d-6062-4a4c-905a-b9ffe5e09325" />
<img width="1275" height="722" alt="Database_construction_Fig2" src="https://github.com/user-attachments/assets/3c8c4432-24ac-4053-8c34-e7ac726a7e02" />

### AOP-Wiki
<img width="1734" height="1456" alt="Database_construction_aopwiki" src="https://github.com/user-attachments/assets/37c3ed02-02c3-4576-b6e0-40ddcac1d707" />

### ToxCast-AOPWiki bridge
<img width="895" height="546" alt="Database_construction_neo4j" src="https://github.com/user-attachments/assets/68266b39-5587-42c6-ad79-1e7600f88446" />
