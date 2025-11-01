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
        """Batch insert product vectors from a CSV file into a vector table."""
        if not self.engine:
            print("No active DB engine.")
            return

        try:
            from sqlalchemy import text 
            import csv
            import json

            with self.engine.connect() as conn, open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                count = 0

                for row in reader:
                    name = row["name"].strip()
                    description = row["description"].strip()
                    embedding_raw = row["embedding"].strip() 

                    try:
                        json.loads(embedding_raw)
                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON embedding for product '{name}': {embedding_raw}")
                        continue

                    sql = f"""
                        INSERT INTO {table_name} (name, description, embedding)
                        VALUES (:name, :description, VEC_FromText(:embedding));
                    """

                    conn.execute(
                        text(sql),
                        
                        {"name": name, "description": description, "embedding": embedding_raw}
                    )
                    count += 1

                conn.commit()
                print(f"Successfully inserted {count} vectors from {file_path} into {table_name}.")

        except Exception as e:
            print(f"Error inserting vectors: {e}")
    
     
     
                 


    def close(self):
        """Dispose of SQLAlchemy engine."""
        if self.engine:
            self.engine.dispose()
            print("Engine disposed and connection closed.")
