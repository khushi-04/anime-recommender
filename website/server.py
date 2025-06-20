# server.py: runs spark and then uses DB to read in user recommendations
import os
import subprocess
import json
from flask import Flask, request, jsonify, render_template

app = Flask(__name__, static_folder="static", static_url_path="/static")

SPARK_SCRIPT = "/home/cs179g/als_model.py"
QUERY_SCRIPT = "/home/cs179g/query_recs.py"

# running spark script
@app.route("/api/run-spark", methods=["POST"])
def run_spark():
    print("DEBUG: Running ALS Spark job...", flush=True)
    data = request.json or {}
    anime = data.get("anime", [])
    genres = data.get("genres", [])

    cmd = [
        "spark-submit",
        "--master", "local[2]",
        "--driver-memory", "32g",
        "--executor-memory", "32g",
        "--conf", "spark.sql.shuffle.partitions=50",
        "--conf", "spark.memory.offHeap.enabled=true",
        "--conf", "spark.memory.offHeap.size=16g",
        "--jars",  "/home/cs179g/sqlite-jdbc-3.49.1.0.jar",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        SPARK_SCRIPT,
        "--anime", *anime,
        "--genres", *genres
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)

    if proc.returncode != 0:
        return jsonify(status="error", stdout=proc.stdout, stderr=proc.stderr), 500

    return jsonify(status="done")

# python script to query recommendations
@app.route("/api/get-recommendations", methods=["POST"])
def get_recs():
    data = request.json or {}
    mode = data.get("mode", "normal")

    cmd = ["python3", QUERY_SCRIPT, mode]
    
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        recommendations = json.loads(proc.stdout)
        return jsonify(recommendations=recommendations)
    except subprocess.CalledProcessError as e:
        return jsonify(error="Script error", stdout=e.stdout, stderr=e.stderr), 500
    except json.JSONDecodeError as e:
        return jsonify(error="Invalid JSON output", message=str(e), output=proc.stdout), 500

@app.route("/")
def home():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
