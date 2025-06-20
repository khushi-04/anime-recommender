import sqlite3

conn = sqlite3.connect("als_with_clusters.db")
cur  = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()
print("Tables:", tables)

# schema of the user_recommendations table
table = "user_recommendations"
cur.execute(f"PRAGMA table_info({table});")
schema = cur.fetchall()
print(f"\nSchema for '{table}':")
for col in schema:
    print("  ", col)

# first 5 rows
print(f"\nFirst 5 rows from '{table}':")
cur.execute(f"SELECT * FROM {table} LIMIT 5;")
for row in cur.fetchall():
    print("  ", row)

# last 5 rows
print(f"\nLast 5 rows from '{table}':")
cur.execute(f"""
    SELECT * 
      FROM {table}
 ORDER BY ROWID DESC
     LIMIT 5
""")
for row in cur.fetchall()[::-1]:
    print("  ", row)

# total row count
cur.execute(f"SELECT COUNT(*) FROM {table};")
count = cur.fetchone()[0]
print(f"\nTotal rows in '{table}': {count}")

conn.close()