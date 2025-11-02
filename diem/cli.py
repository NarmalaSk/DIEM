import argparse
import json
import os
import sys
from diem import DIEM

CONFIG_PATH = os.path.expanduser("~/.diem_config")
db = None

def save_config(uri):
    with open(CONFIG_PATH, "w") as f:
        json.dump({"uri": uri}, f)

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f).get("uri")
    return None

def init_db():
    """Helper to load config and initialize DB."""
    global db
    uri = load_config()
    if not uri:
        print("Error: Not connected. Run 'python cli.py connect' first.")
        return False
    
    if db is None:
        db = DIEM(uri)
        if db.engine is None:
            return False
    return True

def main():
    global db

    parser = argparse.ArgumentParser(description="DIEM CLI - Manage Vector Tables in MariaDB")
    parser.add_argument(
        "action",
        choices=["connect", "create_table", "close", "insert_vector", 
                 "insert_batch", "search", "list_databases", 
                 "list_tables", "get_all", "delete_vectors",
                 "update_vector", "delete_table" , "help"],
        help="Action to perform"
    )
    
    # Args for update/delete
    parser.add_argument("--where", help="SQL WHERE clause (e.g., \"name = :key\")")
    parser.add_argument("--params", help="JSON string of WHERE parameters (e.g., '{\"key\": \"value\"}')")
    
    # filters
    parser.add_argument("--pattern", help="LIKE pattern for listing (e.g., 'test_%')")

    # General args
    parser.add_argument("--table", help="Table name for the action")

    # create_table args
    parser.add_argument("--dim", type=int, help="Vector dimension (e.g., 1536)")
    parser.add_argument("--other_columns", help="JSON string of other columns (e.g., '{\"name\": \"VARCHAR(128)\"}' )")
    parser.add_argument("--primary_key", help="Name of the primary key column")
    parser.add_argument("--distance", default="cosine", help="Distance metric: cosine or euclidean")
    parser.add_argument("--m", type=int, default=8, help="HNSW parameter M (default: 8)")

    # insert_vector args
    parser.add_argument("--data", help="JSON string of the row to insert (e.g., '{\"name\": \"Item\", \"embedding\": [0.1, 0.2]}' )")

    # insert_batch args
    parser.add_argument("--file", help="Path to CSV file for batch insertion")

    # search args
    parser.add_argument("--query_vector", help="JSON string of the query vector (e.g., '[0.1, 0.2, 0.3]' )")
    parser.add_argument("--k", type=int, default=5, help="Number of results to return (default: 5)")
   

    args = parser.parse_args()

    
    try:
        import pymysql
        pymysql.install_as_MySQLdb()
    except ImportError:
        print("pymysql not found. Please run 'pip install pymysql'.")
        


    if args.action == "connect":
        uri = input("Enter MariaDB URI (mariadb://user:pass@host/db): ").strip()
        save_config(uri)
        db = DIEM(uri)

    elif args.action == "create_table":
        if not init_db(): return
        if not args.table or not args.dim:
            print("Error: --table and --dim are required.")
            return
        
        other_columns_dict = None
        if args.other_columns:
            try:
                other_columns_dict = json.loads(args.other_columns)
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON in --other_columns: {args.other_columns}")
                return
                
        db.create_table(
            table_name=args.table,
            vector_dim=args.dim,
            other_columns=other_columns_dict,
            primary_key=args.primary_key,
            distance_metric=args.distance,
            m=args.m
        )

    elif args.action == "insert_vector":
        if not init_db(): return
        if not args.table or not args.data:
            print("Error: --table and --data are required.")
            return

        data_dict = None
        try:
            data_dict = json.loads(args.data)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in --data: {args.data}")
            return
            
        db.insert_vector(args.table, data_dict)

    elif args.action == "insert_batch":
        if not init_db(): return
        if not args.table or not args.file:
            print("Error: --table and --file are required.")
            return
        db.batch_insert_vectors(args.table, args.file)

    elif args.action == "search":
        if not init_db(): return
        if not args.table or not args.query_vector:
            print("Error: --table and --query_vector are required.")
            return
        
        query_vector_list = None
        try:
            query_vector_list = json.loads(args.query_vector)
        except json.JSONDecodeError:
            print(f" Error: Invalid JSON in --query_vector: {args.query_vector}")
            return
            
        results = db.similarity_search(
            table_name=args.table,
            query_vector=query_vector_list,
            distance_metric=args.distance,
            top_k=args.k
        )
        
        if results:
            print(f"Found {len(results)} results:")
            print(json.dumps(results, indent=2))

    elif args.action == "delete_vectors":
        if not init_db(): return
        if not args.table or not args.where:
            print("Error: --table and --where are required for safety.")
            return
        
        where_params = {}
        if args.params:
            try:
                where_params = json.loads(args.params)
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON in --params: {args.params}")
                return
        
        db.delete_vectors(args.table, args.where, where_params)

    elif args.action == "update_vector":
        if not init_db(): return
        if not args.table or not args.where or not args.data:
            print("Error: --table, --where, and --data are required.")
            return
        
        data_dict = {}
        try:
            data_dict = json.loads(args.data)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON in --data: {args.data}")
            return

        where_params = {}
        if args.params:
            try:
                where_params = json.loads(args.params)
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON in --params: {args.params}")
                return
        
        db.update_vector(args.table, data_dict, args.where, where_params)

    elif args.action == "delete_table":
        if not init_db(): return
        if not args.table:
            print("Error: --table is required.")
            return
        
        db.delete_table(args.table)



    elif args.action == "list_databases":
        if not init_db(): return
        databases = db.list_databases(args.pattern)
        if databases is not None:
            print(f"Found {len(databases)} databases:")
            for db_name in databases:
                print(f"- {db_name}")

    elif args.action == "list_tables":
        if not init_db(): return
        tables = db.list_tables(args.pattern)
        if tables is not None:
            print(f"Found {len(tables)} tables in current database:")
            for table_name in tables:
                print(f"- {table_name}")

    elif args.action == "get_all":
        if not init_db(): return
        if not args.table:
            print(" Error: --table is required.")
            return
        
        rows = db.get_all_from_table(args.table)
        if rows:
            print(f"Found {len(rows)} rows in '{args.table}':")
            print(json.dumps(rows, indent=2))
    
    elif args.action == "close":
        uri = load_config()
        if not uri:
            print("No active connection found to close.")
        else:
            if os.path.exists(CONFIG_PATH):
                os.remove(CONFIG_PATH)
            print("Connection configuration cleared.")
        
        if db:
            db.close()


    elif args.action == "help":
         print("""
DIEM - Distributed Embeddings & Analytics Manager CLI

Options:
  --help         Show this message and exit.

Commands:
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
""")
    sys.exit(0)


if __name__ == "__main__":
    main()
