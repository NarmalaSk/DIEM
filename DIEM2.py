"""
DEAM - Distributed Embeddings & Analytics Manager
A simple library for managing vector embeddings in MariaDB with versioning and auditing.
"""

import mysql.connector
from mysql.connector import Error
import csv
import json
import threading
import time
import os
import yaml
import numpy as np
from typing import List, Dict, Optional

def read_yaml(file_path: str) -> dict:
        """
    Reads a YAML file and returns its contents as a dictionary.
    Raises FileNotFoundError if the file doesn't exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"YAML file not found: {file_path}")
    
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
    
        return data

class DEAM:
    """Main class for handling MariaDB connections and vector operations."""
    

    def __init__(self, config_path: str = "config.yaml"):

        creds = read_yaml(config_path)
        

        user = creds.get("user")
        password = creds.get("password")
        database = creds.get("database")
        socket_path = creds.get("socket_path")


        self.connection_params = {
            "user": user,
            "password": password,
            "database": database,
            "unix_socket": socket_path
        }
        self.connection = None

    # -------------------- Connection --------------------
    def connect_db(self) -> bool:
        """Connect to MariaDB via UNIX socket."""
        try:
            self.connection = mysql.connector.connect(**self.connection_params)
            if self.connection.is_connected():
                print(f"Connected to MariaDB via UNIX socket ({self.connection_params['unix_socket']})")
                cursor = self.connection.cursor()
                cursor.execute("SELECT VERSION();")
                version = cursor.fetchone()
                print(f"MariaDB version: {version[0]}")
                cursor.close()
                return True
        except Error as e:
            print(f"Database connection failed: {e}")
            self.connection = None
            return False

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Database connection closed.")

    def _ensure_connection(self):
        if not self.connection or not self.connection.is_connected():
            raise ConnectionError("No active database connection. Call connect_db() first.")

    # -------------------- Table Operations --------------------
    def create_index(self, table_name: str, vector_dim: int):
        self.connect_db()
        """Create a vector table."""
        self._ensure_connection()
        sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                vector VECTOR({vector_dim}),
                metadata JSON,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB;
        """
        cursor = self.connection.cursor()
        cursor.execute(sql)
        cursor.close()
        print(f"Vector table '{table_name}' created with dimension {vector_dim}.")

    # -------------------- Insert Operations --------------------
    def insert_vectors(self, table_name: str, vectors: List[List[float]], metadata: List[Dict]):
        self._ensure_connection()
        if len(vectors) != len(metadata):
            raise ValueError("Length of vectors and metadata must match.")

        sql = f"INSERT INTO {table_name} (vector, metadata) VALUES (%s, %s)"
        cursor = self.connection.cursor()
        for vec, meta in zip(vectors, metadata):
            if isinstance(vec, np.ndarray):
                vec = vec.tolist()
            cursor.execute(sql, (vec, json.dumps(meta)))
        self.connection.commit()
        cursor.close()
        print(f"Inserted {len(vectors)} vectors into '{table_name}'.")

    def batch_insert_vectors(self, table_name: str, vectors: List[List[float]], metadata: Optional[List[Dict]] = None, batch_size: int = 1000):
        self._ensure_connection()
        if metadata and len(metadata) != len(vectors):
            raise ValueError("Length of metadata must match vectors.")

        sql = f"INSERT INTO {table_name} (vector, metadata) VALUES (%s, %s)"
        cursor = self.connection.cursor()
        for i in range(0, len(vectors), batch_size):
            batch_vec = vectors[i:i+batch_size]
            batch_meta = metadata[i:i+batch_size] if metadata else [None]*len(batch_vec)
            data = []
            for vec, meta in zip(batch_vec, batch_meta):
                if isinstance(vec, np.ndarray):
                    vec = vec.tolist()
                data.append((vec, json.dumps(meta) if meta else None))
            cursor.executemany(sql, data)
            self.connection.commit()
            print(f"Inserted batch of {len(data)} vectors into '{table_name}'.")
        cursor.close()
        print(f"Total inserted vectors into '{table_name}': {len(vectors)}.")

    # -------------------- Update/Delete --------------------
    def update_vectors(self, table_name: str, ids: List[int], new_vectors: Optional[List[List[float]]] = None, new_metadata: Optional[List[Dict]] = None):
        self._ensure_connection()
        if (new_vectors and len(new_vectors) != len(ids)) or (new_metadata and len(new_metadata) != len(ids)):
            raise ValueError("Length of updates must match length of IDs.")

        cursor = self.connection.cursor()
        for i, vec_id in enumerate(ids):
            updates = []
            if new_vectors:
                vec = new_vectors[i]
                if isinstance(vec, np.ndarray):
                    vec = vec.tolist()
                updates.append(f"vector = '{vec}'")
            if new_metadata:
                updates.append(f"metadata = '{json.dumps(new_metadata[i])}'")
            if updates:
                update_clause = ", ".join(updates)
                sql = f"UPDATE {table_name} SET {update_clause} WHERE id = {vec_id}"
                cursor.execute(sql)
        self.connection.commit()
        cursor.close()
        print(f"Updated {len(ids)} vectors in '{table_name}'.")

    def delete_vectors(self, table_name: str, ids: Optional[List[int]] = None, filter_by: Optional[Dict] = None, delete_all: bool = False):
        self._ensure_connection()
        cursor = self.connection.cursor()
        sql = ""
        if delete_all:
            sql = f"TRUNCATE TABLE {table_name}"
        elif ids:
            ids_str = ", ".join(map(str, ids))
            sql = f"DELETE FROM {table_name} WHERE id IN ({ids_str})"
        elif filter_by:
            conditions = [f"JSON_EXTRACT(metadata, '$.{k}') = '{v}'" for k, v in filter_by.items()]
            sql = f"DELETE FROM {table_name} WHERE " + " AND ".join(conditions)
        else:
            cursor.close()
            print("No deletion criteria provided.")
            return

        cursor.execute(sql)
        self.connection.commit()
        cursor.close()
        print(f"Deleted vectors from '{table_name}'.")

    # -------------------- Search --------------------
    def search_vectors(self, table_name: str, query_vector: List[float], top_k: int = 5, metric: str = "euclidean") -> List:
        self._ensure_connection()
        if metric.lower() == "euclidean":
            operator, order = "<=>", "ASC"
        elif metric.lower() == "cosine":
            operator, order = "<#>", "ASC"
        elif metric.lower() == "ip":
            operator, order = "<->", "DESC"
        else:
            raise ValueError("metric must be 'euclidean', 'cosine', or 'ip'")

        sql = f"""
            SELECT id, 1-(vector {operator} %s) AS similarity_score, metadata
            FROM {table_name}
            ORDER BY vector {operator} %s {order}
            LIMIT %s
        """
        cursor = self.connection.cursor()
        cursor.execute(sql, (query_vector, query_vector, top_k))
        results = cursor.fetchall()
        cursor.close()
        return results

    # -------------------- Analytics --------------------
    def init_analytics(self, source_table: str, cs_table: str, vector_dim: int = 1536):
        self._ensure_connection()
        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {cs_table} (
                id INT PRIMARY KEY,
                vector VECTOR({vector_dim}),
                metadata JSON,
                similarity_score FLOAT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=Columnstore;
        """
        cursor = self.connection.cursor()
        cursor.execute(sql_create)
        cursor.close()
        print(f"Analytics table '{cs_table}' created.")

        threading.Thread(target=self._async_sync_to_columnstore, args=(source_table, cs_table), daemon=True).start()
        print(f"Started async sync from '{source_table}' to '{cs_table}'.")

    def _async_sync_to_columnstore(self, source_table: str, cs_table: str, batch_size: int = 1000, interval: int = 5):
        while True:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {source_table} LIMIT {batch_size}")
            rows = cursor.fetchall()
            if not rows:
                cursor.close()
                time.sleep(interval)
                continue

            insert_sql = f"INSERT INTO {cs_table} (id, vector, metadata, similarity_score, created_at) VALUES (%s,%s,%s,%s,%s)"
            for row in rows:
                vec = row['vector']
                if isinstance(vec, str):
                    vec = json.loads(vec)
                vec_norm = float(np.linalg.norm(vec))
                cursor.execute(insert_sql, (row['id'], vec, row['metadata'], vec_norm, row['created_at']))

            self.connection.commit()
            cursor.close()
            print(f"Synced {len(rows)} vectors from '{source_table}' to '{cs_table}'.")
            time.sleep(interval)

    def run_analytics(self, cs_table: str):
        self._ensure_connection()
        cursor = self.connection.cursor()
        cursor.execute(f"""
            SELECT JSON_EXTRACT(metadata, '$.user') AS user_id,
                   COUNT(*) AS total_vectors
            FROM {cs_table}
            GROUP BY user_id
            ORDER BY total_vectors DESC;
        """)
        print("Vectors per user:")
        for row in cursor.fetchall():
            print(row)

        cursor.execute(f"SELECT AVG(similarity_score) AS avg_score FROM {cs_table};")
        avg_score = cursor.fetchone()
        print("Average similarity score:", avg_score[0])
        cursor.close()

    # -------------------- Utility --------------------
    def list_tables(self) -> List[str]:
        self._ensure_connection()
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
