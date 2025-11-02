import csv
import json
from sqlalchemy import create_engine, text, inspect

class DIEM:
    def __init__(self, uri):
        """Initialize SQLAlchemy engine."""
        try:
            self.engine = create_engine(uri)
            with self.engine.connect() as conn:
                print(" Connected to MariaDB successfully.")
        except Exception as e:
            print(f" Error connecting to MariaDB: {e}")
            self.engine = None

    

    def create_table(self, table_name, vector_dim, 
                     other_columns=None, 
                     primary_key=None,
                     distance_metric="cosine", 
                     m=8, 
                     index_name="vec_idx"):
        
        if not self.engine:
            print("No active DB engine.")
            return

 
        try:
            inspector = inspect(self.engine)
            if inspector.has_table(table_name):
                print(f"Table '{table_name}' already exists.")
                return  
        except Exception as e:
            print(f"Error checking for table: {e}")
            return
        
        if not table_name.isidentifier():
            raise ValueError(f"Invalid table name: {table_name}")
        if not index_name.isidentifier():
            raise ValueError(f"Invalid index name: {index_name}")
        if distance_metric.lower() not in ["cosine", "euclidean"]:
            raise ValueError("Invalid distance metric. Choose 'cosine' or 'euclidean'.")

        column_definitions = []
        if other_columns:
            for col_name, col_type in other_columns.items():
                if not col_name.isidentifier():
                    raise ValueError(f"Invalid column name: {col_name}")
                column_definitions.append(f"    `{col_name}` {col_type}") 

        column_definitions.append(f"    embedding VECTOR({vector_dim}) NOT NULL")

        if primary_key:
            if not primary_key.isidentifier():
                 raise ValueError(f"Invalid primary key column name: {primary_key}")
            if primary_key not in (other_columns or {}):
                raise ValueError(f"Primary key '{primary_key}' is not defined in 'other_columns'.")
            
            column_definitions.append(f"    PRIMARY KEY (`{primary_key}`)") 

        column_definitions.append(f"    VECTOR INDEX `{index_name}` (embedding) M=:m DISTANCE={distance_metric.upper()}")

      
        sql = f"""
        CREATE TABLE {table_name} (
        {',n'.join(column_definitions)}
        ) ENGINE=InnoDB;"""

        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql), {"m": m})
                conn.commit()
            # MODIFIED: Print a specific "created" message
            print(f"Table '{table_name}' created successfully.")
        except Exception as e:
            # This will now only catch actual errors
            print(f"Error creating table: {e}")

    

    def insert_vector(self, table_name, data):
        
        if not self.engine:
            print(" No active DB engine.")
            return

        if not table_name.isidentifier():
            print(f"Error: Invalid table name '{table_name}'.")
            return
        if 'embedding' not in data:
            print("Error: 'data' dictionary must contain an 'embedding' key.")
            return

        params = data.copy()
        try:
            params['embedding'] = json.dumps(params['embedding'])
        except TypeError as e:
            print(f"Error: 'embedding' value must be a JSON-serializable list. {e}")
            return

        columns = []
        value_placeholders = []
        for col_name in params.keys():
            if not col_name.isidentifier():
                print(f"Error: Invalid column name '{col_name}'.")
                return
            
            columns.append(col_name)
            if col_name == 'embedding':
                value_placeholders.append("VEC_FromText(:embedding)")
            else:
                value_placeholders.append(f":{col_name}")

        columns_sql = ", ".join(columns)
        values_sql = ", ".join(value_placeholders)

        sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({values_sql});"

        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql), params)
                conn.commit()
            print(f"Successfully inserted 1 row into {table_name}.")
        except Exception as e:
            print(f"Error inserting vector: {e}")

    
    # BATCH INSERT FROM CSV
    def batch_insert_vectors(self, table_name, file_path):
        
        if not self.engine:
            print("No active DB engine.")
            return

        try:
            with self.engine.connect() as conn, open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                if not reader.fieldnames:
                    print(f"Error: CSV file '{file_path}' is empty or invalid.")
                    return
                
                if 'embedding' not in reader.fieldnames:
                    print(f"Error: CSV file must have an 'embedding' column.")
                    return


                columns = [col for col in reader.fieldnames if col.isidentifier()]
                other_cols = [col for col in columns if col != 'embedding']
                
                placeholders = []
                for col in columns:
                    if col == 'embedding':
                        placeholders.append("VEC_FromText(:embedding)")
                    else:
                        placeholders.append(f":{col}")
                
                columns_sql = ", ".join(columns)
                values_sql = ", ".join(placeholders)
                
                sql = f"""
                    INSERT INTO {table_name} ({columns_sql})
                    VALUES ({values_sql});
                """
                sql_statement = text(sql)
                
                count = 0
                transaction = conn.begin()
                try:
                    for row in reader:
                        params = {col: row[col] for col in columns}
                        
                        try:
                            json.loads(params['embedding'])
                        except json.JSONDecodeError:
                            print(f"Skipping invalid JSON embedding in row: {row}")
                            continue
                        
                        conn.execute(sql_statement, params)
                        count += 1
                    
                    transaction.commit()
                    print(f"Successfully inserted {count} vectors from {file_path} into {table_name}.")
                
                except Exception as e:
                    transaction.rollback()
                    print(f"Error during batch insert (rolled back): {e}")

        except FileNotFoundError:
            print(f"Error: File not found at '{file_path}'")
        except Exception as e:
            print(f"Error inserting vectors: {e}")

    
    
    # SEARCH
    def similarity_search(self, table_name, query_vector, distance_metric, top_k=5):
        
        if not self.engine:
            print(" No active DB engine.")
            return None

        dist_sql = ""

        if distance_metric.lower() == 'cosine':
            dist_sql = "VEC_DISTANCE_COSINE(embedding, VEC_FromText(:query_vec))"
        
        elif distance_metric.lower() == 'euclidean':
            dist_sql = "VEC_DISTANCE_EUCLIDEAN(embedding, VEC_FromText(:query_vec))"
        
        else:
            print("Error: Invalid distance metric. Choose 'cosine' or 'euclidean'.")
            return None

        
        try:
            query_vec_json = json.dumps(query_vector)
        except TypeError as e:
            print(f"Error: 'query_vector' must be a JSON-serializable list. {e}")
            return None
            
        sql = f"""
            SELECT *,
                   {dist_sql} AS distance
            FROM {table_name}
            ORDER BY distance ASC
            LIMIT :top_k;
        """
        
       
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), {
                    "query_vec": query_vec_json,
                    "top_k": top_k
                })
                
                rows = [dict(row._mapping) for row in result.fetchall()]
                
                for row in rows:
                    if 'embedding' in row:
                        row['embedding'] = str(row['embedding'])
                
                return rows
                
        except Exception as e:
            print(f"Error during similarity search: {e}")
            return None


    # DELETE VECTOR
    def delete_vectors(self, table_name, where_clause, params=None):
        
        if not self.engine:
            print("No active DB engine.")
            return False
        if not table_name.isidentifier():
            print(f"Error: Invalid table name '{table_name}'.")
            return False
        if not where_clause:
            print("Error: A WHERE clause is required to prevent accidental mass deletion.")
            return False

        sql = f"DELETE FROM {table_name} WHERE {where_clause};"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or {})
                conn.commit()
                print(f"Successfully deleted {result.rowcount} row(s) from {table_name}.")
                return True
        except Exception as e:
            print(f"Error deleting vectors: {e}")
            return False

    # UPDATE VECTOR
    def update_vector(self, table_name, data, where_clause, params=None):
        
        if not self.engine:
            print("No active DB engine.")
            return False
        if not table_name.isidentifier():
            print(f"Error: Invalid table name '{table_name}'.")
            return False
        if not where_clause:
            print("Error: A WHERE clause is required to prevent accidental mass update.")
            return False
        if not data:
            print("Error: No data provided to update.")
            return False

   
        set_params = {}
        set_expressions = []

        for col_name, value in data.items():
            if not col_name.isidentifier():
                print(f"Error: Invalid column name '{col_name}'.")
                return False
            
            param_name = f"set_{col_name}" 
            
            if col_name == 'embedding':
                set_expressions.append(f"embedding = VEC_FromText(:{param_name})")
                try:
                    set_params[param_name] = json.dumps(value)
                except TypeError:
                    print(f"Error: 'embedding' value must be a JSON-serializable list.")
                    return False
            else:
                set_expressions.append(f"{col_name} = :{param_name}")
                set_params[param_name] = value

        set_sql = ", ".join(set_expressions)
        sql = f"UPDATE {table_name} SET {set_sql} WHERE {where_clause};"
        
        
        final_params = set_params.copy()
        final_params.update(params or {})

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), final_params)
                conn.commit()
                if result.rowcount == 0:
                    print(f"⚠️ Warning: Update ran but 0 rows matched the WHERE clause in {table_name}.")
                else:
                    print(f"Successfully updated {result.rowcount} row(s) in {table_name}.")
                return True
        except Exception as e:
            print(f"Error updating vector: {e}")
            return False

    # DELETE TABLE
    def delete_table(self, table_name):
        """
        Deletes (drops) an entire table. This is permanent.
        """
        if not self.engine:
            print("No active DB engine.")
            return False
        if not table_name.isidentifier():
            print(f"Error: Invalid table name '{table_name}'.")
            return False

        # Get user confirmation from the command line
        confirm = input(f"Are you sure you want to permanently delete the table '{table_name}'? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Aborted. Table was not deleted.")
            return False

        sql = f"DROP TABLE IF EXISTS {table_name};"
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
            print(f"Successfully deleted table '{table_name}'.")
            return True
        except Exception as e:
            print(f"Error deleting table: {e}")
            return False


    
    # LIST DATABASE
    def list_databases(self, like_pattern=None):
        """Lists databases on the server."""
        if not self.engine:
            print(" No active DB engine.")
            return None
        
        sql = "SHOW DATABASES"
        params = {}
        if like_pattern:
            sql += " LIKE :pattern"
            params = {"pattern": like_pattern}
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params)
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f" Error listing databases: {e}")
            return None

    
    # LIST TABLES
    def list_tables(self, like_pattern=None):
        """Lists tables in the currently connected database."""
        if not self.engine:
            print(" No active DB engine.")
            return None

        sql = "SHOW TABLES"
        params = {}
        if like_pattern:
            sql += " LIKE :pattern"
            params = {"pattern": like_pattern}

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params)
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f" Error listing tables: {e}")
            return None

    
    # GET
    def get_all_from_table(self, table_name):
        """Fetches all rows and vectors from a specific table."""
        if not self.engine:
            print(" No active DB engine.")
            return None
        if not table_name.isidentifier():
            print(f" Error: Invalid table name '{table_name}'.")
            return None
        
        sql = f"SELECT * FROM {table_name};" 
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                rows = [dict(row._mapping) for row in result.fetchall()]
                
                
                for row in rows:
                    if 'embedding' in row:
                        row['embedding'] = str(row['embedding'])
                return rows
        except Exception as e:
            print(f" Error fetching from table: {e}")
            return None

    # List storage Engines
    def storage_engines(self):
        """list Storage Engine Installed in MariaDB."""
        if not self.engine:
            print("No active Engine.")
            return

        sql = "SHOW ENGINES;"
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                engines = [dict(row._mapping) for row in result.fetchall()]
                print("Available Storage Engines:")
                for engine in engines:
                    print(f" - {engine['Engine']}: {engine['Support']}")
                
        except Exception as e:
            print(f"Error verifying Spider engine: {e}")

    def install_storage_engine(self,storage):
        """Install Storage Engine in MariaDB."""
        if not self.engine:
            print("No active Engine.")
            return

        sql = f"INSTALL SONAME '{storage}';"
        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()
                print(f"Storage Engine '{storage}' installed successfully.")
        except Exception as e:
            print(f"Error installing Storage Engine '{storage}': {e}")

