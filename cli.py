import argparse
import json
import os
from DIEM import DIEM

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

def main():
    global db

    parser = argparse.ArgumentParser(description="DIEM CLI - Manage Vector Tables in MariaDB")
    parser.add_argument(
        "action",
        choices=["connect", "create_table", "close" , "insert_vectors", "insert_batch"],
        help="Action to perform"
    )
    parser.add_argument("--table", help="Table name (required for create_table)")
    parser.add_argument("--dim", type=int, help="Vector dimension (e.g., 1536)")
    parser.add_argument("--distance", default="cosine", help="Distance metric: cosine or euclidean")
    parser.add_argument("--m", type=int, default=8, help="HNSW parameter (default: 8)")
    parser.add_argument("--embedding", type=list)
    parser.add_argument("--doc_id", type=int, default=8, help="HNSW parameter (default: 8)")
    parser.add_argument("--file", help="Path to CSV file for batch insertion")


    args = parser.parse_args()

    if args.action == "connect":
        uri = input("Enter MariaDB URI: ").strip()
        save_config(uri)
        db = DIEM(uri)
        print("Database connection initialized and saved.")

    elif args.action == "create_table":
        uri = load_config()
        if not uri:
            print("Error: Not connected. Run 'python3 cli.py connect' first.")
            return
        db = DIEM(uri)
        if not args.table or not args.dim:
            print("Error: --table and --dim are required for create_table action.")
            return
        db.create_table(args.table, args.dim, args.distance, args.m)

    elif args.action == "close":
        uri = load_config()
        if not uri:
            print("Error: No active connection found.")
            return
        db = DIEM(uri)
        db.close()
        os.remove(CONFIG_PATH)
        print("Connection closed and configuration cleared.")
    

    elif args.action == "insert_vectors":
         uri = load_config()
         if not uri:
            print("Error: Not connected. Run 'python3 cli.py connect' first.")
            return
         db = DIEM(uri)
         db.insert_vectors(args.doc_id , args.table , args.embedding)  


    elif args.action == "insert_batch":
         uri = load_config()
         if not uri:
            print("Error: Not connected. Run 'python3 cli.py connect' first.")
            return
         db = DIEM(uri)
         if not args.table or not args.file:
            print("Error: --table and --file are required for insert_batch action.")
            return
         db.batch_insert_vectors(args.table, args.file)

         
if __name__ == "__main__":
    main()
