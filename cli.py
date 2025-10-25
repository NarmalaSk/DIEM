#!/usr/bin/env python3
"""
DEAM CLI - Command Line Interface for Distributed Embeddings & Analytics Manager
"""

import click
import json
import numpy as np
from DIEM2 import DEAM  # Assuming your DEAM class is in diem.py

# Initialize DEAM instance
deam = DEAM()


@click.group(help="DEAM - Distributed Embeddings & Analytics Manager CLI")
@click.option('--config', default="config.yaml", help="Path to the database config YAML file.")
def cli(config):
    """Initialize DEAM with config file."""

    deam.config = deam.__init__(config_path=config)


# ----------------- Connection -----------------
@cli.command(help="Connect to MariaDB (TCP/IP or UNIX socket).")
def connect():

    if deam.connect_db():
        click.echo("Connection established.")
    else:
        click.echo("Failed to connect.")


@cli.command(help="Close active database connection.")
def disconnect():
    deam.close_connection()


# ----------------- Table Operations -----------------
@cli.command(help="List all tables in the connected database.")
def list_tables():
    tables = deam.list_tables()
    click.echo("Tables:")
    for t in tables:
        click.echo(f" - {t}")


@cli.command(help="Create a vector table / index.")
@click.argument("table_name")
@click.option("--vector-dim", default=1536, help="Dimension of the vector column.")
def create_index(table_name, vector_dim):
    deam.create_index(table_name, vector_dim)


# ----------------- Vector CRUD -----------------
@cli.command(help="Insert vectors and metadata into a table.")
@click.argument("table_name")
@click.option("--vectors", help="JSON array of vectors (list of lists).")
@click.option("--metadata", help="JSON array of metadata dicts.")
@click.option("--csv-file", help="Optional CSV file with vector and metadata columns.")
def insert_vectors(table_name, vectors, metadata, csv_file):
    vecs = json.loads(vectors) if vectors else None
    meta = json.loads(metadata) if metadata else None
    deam.Insert_vectors(table_name, vectors=vecs, metadata=meta, csv_file=csv_file)


@cli.command(help="Delete vectors from a table.")
@click.argument("table_name")
@click.option("--ids", help="Comma-separated IDs to delete.")
@click.option("--filter", "filter_by", help="JSON string to filter metadata.")
@click.option("--all", "delete_all", is_flag=True, help="Delete all vectors.")
def delete_vectors(table_name, ids, filter_by, delete_all):
    id_list = list(map(int, ids.split(","))) if ids else None
    filter_dict = json.loads(filter_by) if filter_by else None
    deam.delete_vectors(table_name, ids=id_list, filter_by=filter_dict, delete_all=delete_all)


@cli.command(help="Update vectors or metadata by IDs.")
@click.argument("table_name")
@click.option("--ids", required=True, help="Comma-separated vector IDs to update.")
@click.option("--vectors", help="JSON array of new vectors.")
@click.option("--metadata", help="JSON array of new metadata.")
def update_vectors(table_name, ids, vectors, metadata):
    id_list = list(map(int, ids.split(",")))
    vecs = json.loads(vectors) if vectors else None
    meta = json.loads(metadata) if metadata else None
    deam.update_vectors(table_name, id_list, new_vectors=vecs, new_metadata=meta)


@cli.command(help="Search for similar vectors in a table.")
@click.argument("table_name")
@click.option("--query", required=True, help="JSON array of query vector.")
@click.option("--top-k", default=5, help="Number of top similar vectors to return.")
@click.option("--metric", default="euclidean", help="Similarity metric: euclidean, cosine, ip")
def search_vectors(table_name, query, top_k, metric):
    query_vec = json.loads(query)
    results = deam.search_vectors(table_name, query_vec, top_k=top_k, metric=metric)
    click.echo("Search Results:")
    for r in results:
        click.echo(r)


# ----------------- Analytics -----------------
@cli.command(help="Initialize analytics ColumnStore table from existing table.")
@click.argument("source_table")
@click.argument("cs_table")
@click.option("--vector-dim", default=1536, help="Dimension of the vector column.")
def init_analytics(source_table, cs_table, vector_dim):
    deam.init_analytics(source_table, cs_table, vector_dim)


@cli.command(help="Run analytics queries on ColumnStore table.")
@click.argument("cs_table")
def run_analytics(cs_table):
    deam.run_analytics(cs_table)


# ----------------- Main -----------------
if __name__ == "__main__":
    cli()
