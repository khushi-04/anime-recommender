# old server.py that used a csv file to read in the user recommendations
import os
import json
import pandas as pd
import subprocess
from flask import Flask, request, jsonify, render_template

app = Flask(
    __name__,
    static_folder="static",
    static_url_path="/static"
)

BASE_DIR = os.path.dirname(__file__)
SPARK_SCRIPT = "/home/cs179g/als_model.py"
recs = "/home/cs179g/output_user_recommendations.csv"

@app.route("/api/run-spark", methods=["POST"])
def run_spark():
    print("DEBUG: SPARK_SCRIPT =", SPARK_SCRIPT, flush=True)
    print("DEBUG: exists?     ", os.path.exists(SPARK_SCRIPT), flush=True)

    from shutil import which
    print("DEBUG: spark-submit path (which):", which("spark-submit"), flush=True)

    print("DEBUG: got POST to /api/run-spark", flush=True)
    data = request.json or {}
    print("DEBUG: payload:", data, flush=True)

    data = request.json or {}
    anime = data.get("anime", [])
    genres = data.get("genres", [])

    cmd = ["spark-submit",
    "--master", "local[2]",
    "--driver-memory", "32g",
    "--executor-memory", "32g",
    "--conf", "spark.sql.shuffle.partitions=50",
    "--conf", "spark.memory.offHeap.enabled=true",
    "--conf", "spark.memory.offHeap.size=16g",
    "--jars",  "/home/cs179g/sqlite-jdbc-3.49.1.0.jar",
    "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer", SPARK_SCRIPT, 
    "--anime", *anime, "--genres", *genres]
    print("DEBUG: running command:", cmd, flush=True)

    proc = subprocess.run(cmd, capture_output=True, text=True)

    if proc.returncode != 0:
        print("=== SPARK STDOUT ===", flush=True)
        print(proc.stdout, flush=True)
        print("=== SPARK STDERR ===", flush=True)
        print(proc.stderr, flush=True)
        print("====================", flush=True)
        return jsonify(status="error", stdout=proc.stdout, stderr=proc.stderr), 500

    print("DEBUG: spark-submit succeeded", flush=True)

    return jsonify(status="done")

@app.route("/api/get-recommendations", methods=["POST"])
def get_recs():
    data = request.json or {}
    mode = data.get("mode", "normal")
    cluster = data.get("cluster", None)
    try:
        df = pd.read_csv(recs)
    except Exception as e:
        return jsonify(error=f"Failed to load CSV: {str(e)}"), 500

    if mode == "normal":
        top_df = df.head(5)
    elif mode == "personalize":
        if cluster is None:
            return jsonify(error="'cluster' is required for personalize"), 400
        filtered = df[df["cluster"] == cluster]
        top_df   = filtered.head(5)
    else:
        return jsonify(error=f"Unknown mode: {mode}"), 400

    results = top_df.to_dict(orient="records")
    return jsonify(recommendations=results)

@app.route("/")
def home():
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
