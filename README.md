### DIEM — Distributed Embedding and Analytics Manager
#### Introduction
_____________________________________________________________________________________________________________________________________________________________________________________________
DIEM is a CLI tool built using Python and the MariaDB SQL connector. It simplifies CRUD operations on vector embeddings and supports semantic search, cosine similarity, and other distance-based analytics.

The system leverages MariaDB 11.8 GA LTS! features such as Vector Data Type, Spider Engine .Together, these enable high-performance, distributed vector management and analytics.

### Prequesites

Mariadb Db version 11.8 GA Lts (greater or 11.8)

### Installation
Install using pip 
```
pip install diemcli
```
Clone git repo
```
git clone https://github.com/NarmalaSk/DIEM.git
```

### Connect to Mariadb 

```
diem connect
```
##### Enter Connection String


### Architecture
_____________________________________________________________________________________________________________________________________________________________________________________________





All database interactions are abstracted through DIEM.py, which translates SQL operations into intuitive CLI commands.
This abstraction eliminates the need for manual SQL query handling, making vector data operations faster and more user-friendly.


### Distributed System
_____________________________________________________________________________________________________________________________________________________________________________________________
DIEM automatically distributes vector data across the cluster using the MariaDB Spider Engine, ensuring:

Horizontal scalability

Load-balanced queries

Transparent data retrieval

Using DIEM CLI commands, developers can easily manage data distribution, replication, and query execution across nodes—without manually configuring Spider partitions or routes.

### Integration in ML Model Development
_____________________________________________________________________________________________________________________________________________________________________________________________
DIEM bridges the gap between data storage and machine learning workflows by providing an efficient interface to:

Store and retrieve embeddings generated from ML models.

Perform semantic search for model evaluation and feature comparison.

Leverage MariaDB’s ColumnStore for analytics on large-scale vector datasets.

This makes DIEM an ideal choice for teams developing LLM-based applications, recommendation systems, and semantic similarity engines that need scalable, SQL-native vector management.

### CLI 
_____________________________________________________________________________________________________________________________________________________________________________________________
```Usage: cli.py [OPTIONS] COMMAND [ARGS]...

  DIEM - Distributed Embeddings & Analytics Manager CLI
Optional Parameters:

Arguements:
  connect          Connect to MariaDB and save the connection URI.
  create_table     Create a vector table with given dimensions and metadata.
  insert_vector    Insert a single vector embedding into a table.
  insert_batch     Insert multiple vector embeddings from a CSV file.
  search           Perform similarity search on vector embeddings.
  list_databases   List all databases in the connected MariaDB instance.
  list_tables      List all tables in the connected database.
  get_all          Retrieve all rows from a given table.
  update_vector    Update vector embeddings or metadata using a WHERE clause.
  delete_vectors   Delete vectors from a table with conditions.
  delete_table     Drop a vector table permanently.
  close            Close active database connection and clear config.
```

