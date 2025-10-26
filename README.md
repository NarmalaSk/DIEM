### DIEM — Distributed Embedding and Analytics Manager
#### Introduction
_____________________________________________________________________________________________________________________________________________________________________________________________
DIEM is a CLI tool built using Python and the MariaDB SQL connector. It simplifies CRUD operations on vector embeddings and supports semantic search, cosine similarity, and other distance-based analytics.

The system leverages MariaDB 11.8 LTS features such as:

Vector Data Type

ColumnStore Engine

Spider Engine

Together, these enable high-performance, distributed vector management and analytics.
_____________________________________________________________________________________________________________________________________________________________________________________________
### Architecture

DIEM follows a modular and lightweight architecture, deployable on:

A single-node MariaDB instance, or

A distributed MariaDB cluster

All database interactions are abstracted through DIEM.py, which translates SQL operations into intuitive CLI commands.
This abstraction eliminates the need for manual SQL query handling, making vector data operations faster and more user-friendly.
_____________________________________________________________________________________________________________________________________________________________________________________________
### Authentication

DIEM uses a config.yaml file for cluster authentication and connection configuration. This enables both local and remote vector management.

Supported connection modes:

Unix Socket — For local and intra-cluster communication.

TCP/IP — For remote connections using the host’s IP address.

This flexible authentication model allows seamless integration into existing infrastructure without changing security or connectivity settings.
_____________________________________________________________________________________________________________________________________________________________________________________________
### Distributed System

DIEM automatically distributes vector data across the cluster using the MariaDB Spider Engine, ensuring:

Horizontal scalability

Load-balanced queries

Transparent data retrieval

Using DIEM CLI commands, developers can easily manage data distribution, replication, and query execution across nodes—without manually configuring Spider partitions or routes.
_____________________________________________________________________________________________________________________________________________________________________________________________
### Integration in ML Model Development

DIEM bridges the gap between data storage and machine learning workflows by providing an efficient interface to:

Store and retrieve embeddings generated from ML models.

Perform semantic search for model evaluation and feature comparison.

Leverage MariaDB’s ColumnStore for analytics on large-scale vector datasets.

This makes DIEM an ideal choice for teams developing LLM-based applications, recommendation systems, and semantic similarity engines that need scalable, SQL-native vector management.
_____________________________________________________________________________________________________________________________________________________________________________________________
### CLI Usage
```Usage: cli.py [OPTIONS] COMMAND [ARGS]...

  DIEM - Distributed Embeddings & Analytics Manager CLI

Options:
  --config TEXT  Path to the database config YAML file.
  --help         Show this message and exit.

Commands:
  connect         Connect to MariaDB (TCP/IP or UNIX socket).
  create-index    Create a vector table / index.
  delete-vectors  Delete vectors from a table.
  disconnect      Close active database connection.
  init-analytics  Initialize analytics ColumnStore table from existing data.
  insert-vectors  Insert vectors and metadata into a table.
  list-tables     List all tables in the connected database.
  run-analytics   Run analytics queries on ColumnStore table.
  search-vectors  Search for similar vectors in a table.
  update-vectors  Update vectors or metadata by IDs.```

