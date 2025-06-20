# 1: takes in user input for the kind of recommendation they prefer (normal vs personalized)
# 2: queries database to display query outputs

import sqlite3
import argparse
import json

def main():
    # user input
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["normal", "personalize"],
    )
    args = parser.parse_args()
    mode = args.mode

    conn = sqlite3.connect("/home/cs179g/als_with_clusters.db")
    cur = conn.cursor()

    table = "user_recommendations"

    # normal mode - takes top 5 from database
    if mode == "normal":
        cur.execute(f"SELECT * FROM {table} LIMIT 5;")
        rows = cur.fetchall()

    # mode personalize - takes top 5 within the same cluster
    else:
        cur.execute(f"SELECT cluster FROM {table} LIMIT 1;")
        first = cur.fetchone()
        if first is None:
            print("No rows found in the table.")
            conn.close()
            return

        first_cluster = first[0]

        cur.execute(
            f"SELECT * FROM {table} WHERE cluster = ? LIMIT 5;",
            (first_cluster,)
        )
        rows = cur.fetchall()

    conn.close()
    out = []

    # output as a json since frontend expects a json output for the recommendations
    for row in rows:
        genre, title, cluster = row
        out.append({
            "genre": genre,
            "title": title,
            "cluster": cluster
        })
    print(json.dumps(out))

if __name__ == "__main__":
    main()
