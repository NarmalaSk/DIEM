from sqlalchemy import create_engine, text
import csv
import json


class DIEM:
    def __init__(self, uri):
        """Initialize SQLAlchemy engine."""
        try:
            self.engine = create_engine(uri)
            with self.engine.connect() as conn:
                print("Connected to MariaDB successfully.")
        except Exception as e:
            print(f"Error connecting to MariaDB: {e}")
            self.engine = None

    def create_table(self, table_name, vector_dim, distance_metric="cosine", m=8):
        """Dynamically create a vector table."""
        if not self.engine:
            print(" No active DB engine.")
            return


        if distance_metric.lower() not in ["cosine", "euclidean"]:
            raise ValueError("Invalid distance metric. Choose 'cosine' or 'euclidean'.")

        sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            doc_id BIGINT UNSIGNED PRIMARY KEY,
            embedding VECTOR({vector_dim}) NOT NULL,
            VECTOR INDEX (embedding) M=:m DISTANCE={distance_metric}
        ) ENGINE=InnoDB;
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql), {"m": m})
                conn.commit()
            print(f"Table '{table_name}' created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")
    
    def insert_vectors(self, doc_id, table_name , embedding):
        """Insert vectors into the specified table."""
        if not self.engine:
            print(" No active DB engine.")
            return

        sql = f"""INSERT INTO {table_name} (doc_id, embedding) 
        VALUES ({doc_id}, VEC_FromText({embedding}));"""
        print("Vectors Inserted Successfully")


    def batch_insert_vectors(self, table_name, file_path):
        """Batch insert vectors from a CSV file into a vector table."""
        if not self.engine:
           print("No active DB engine.")
           return

        try:
            with self.engine.connect() as conn, open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    doc_id = int(row["doc_id"])
                    embedding = row["embedding"]

                    embeddingcl = embedding.strip("[]").replace(",", " ")

                    sql = f"""
                    INSERT INTO {table_name} (doc_id, embedding)
                    VALUES ({doc_id}, VEC_FromText('{embeddingcl}'));
                    """
                    conn.execute(text(sql))

                print(f" Successfully inserted vectors from {file_path} into {table_name}.")
                

        except Exception as e:
            print(f"Error inserting vectors: {e}")
            


    def close(self):
        """Dispose of SQLAlchemy engine."""
        if self.engine:
            self.engine.dispose()
            print("Engine disposed and connection closed.")
