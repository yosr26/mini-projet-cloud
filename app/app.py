from flask import Flask, request, jsonify
import psycopg2
import redis
import os
import json
import time
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    decode_responses=True
)

REQUEST_COUNT = Counter(
    'app_request_count_total',
    'Nombre total de requêtes',
    ['method', 'endpoint', 'status']
)
REQUEST_LATENCY = Histogram(
    'app_request_latency_seconds',
    'Latence des requêtes',
    ['endpoint']
)
VISIT_COUNTER = Counter(
    'app_visit_count_total',
    'Nombre total de visites'
)

def get_db():
    """Ouvre une connexion à la base PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        database=os.getenv("DB_NAME", "tasks"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "admin")
    )
    return conn

def init_db():
    """Crée la table 'tasks' si elle n'existe pas encore."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            done BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


@app.route("/")
def index():
    """Page d'accueil — compteur de visites via Redis."""
    VISIT_COUNTER.inc()
    visits = redis_client.incr("visit_count")
    REQUEST_COUNT.labels(method="GET", endpoint="/", status="200").inc()
    return jsonify({
        "message": "Bienvenue sur la TODO API !",
        "visites_totales": visits
    })

@app.route("/tasks", methods=["GET"])
def get_tasks():
    """Retourne toutes les tâches (avec cache Redis 30 sec)."""
    start = time.time()

    # On regarde d'abord dans le cache Redis
    cached = redis_client.get("all_tasks")
    if cached:
        REQUEST_COUNT.labels(method="GET", endpoint="/tasks", status="200").inc()
        REQUEST_LATENCY.labels(endpoint="/tasks").observe(time.time() - start)
        return jsonify({"source": "cache", "tasks": json.loads(cached)})

    # Sinon on interroge PostgreSQL
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, title, done, created_at::text FROM tasks ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    tasks = [{"id": r[0], "title": r[1], "done": r[2], "created_at": r[3]} for r in rows]

    # On met en cache pour 30 secondes
    redis_client.setex("all_tasks", 30, json.dumps(tasks))

    REQUEST_COUNT.labels(method="GET", endpoint="/tasks", status="200").inc()
    REQUEST_LATENCY.labels(endpoint="/tasks").observe(time.time() - start)
    return jsonify({"source": "database", "tasks": tasks})

@app.route("/tasks", methods=["POST"])
def create_task():
    """Crée une nouvelle tâche."""
    start = time.time()
    data = request.get_json()

    if not data or "title" not in data:
        REQUEST_COUNT.labels(method="POST", endpoint="/tasks", status="400").inc()
        return jsonify({"error": "Le champ 'title' est obligatoire"}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tasks (title) VALUES (%s) RETURNING id, title, done, created_at::text",
        (data["title"],)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    # On invalide le cache
    redis_client.delete("all_tasks")

    task = {"id": row[0], "title": row[1], "done": row[2], "created_at": row[3]}
    REQUEST_COUNT.labels(method="POST", endpoint="/tasks", status="201").inc()
    REQUEST_LATENCY.labels(endpoint="/tasks").observe(time.time() - start)
    return jsonify(task), 201

@app.route("/tasks/<int:task_id>", methods=["DELETE"])
def delete_task(task_id):
    """Supprime une tâche par son ID."""
    start = time.time()

    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM tasks WHERE id = %s RETURNING id", (task_id,))
    deleted = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if not deleted:
        REQUEST_COUNT.labels(method="DELETE", endpoint="/tasks/<id>", status="404").inc()
        return jsonify({"error": "Tâche introuvable"}), 404

    # On invalide le cache
    redis_client.delete("all_tasks")

    REQUEST_COUNT.labels(method="DELETE", endpoint="/tasks/<id>", status="200").inc()
    REQUEST_LATENCY.labels(endpoint="/tasks/<id>").observe(time.time() - start)
    return jsonify({"message": f"Tâche {task_id} supprimée"})

@app.route("/tasks/<int:task_id>/done", methods=["PATCH"])
def mark_done(task_id):
    """Marque une tâche comme terminée."""
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE tasks SET done = TRUE WHERE id = %s RETURNING id, title, done",
        (task_id,)
    )
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"error": "Tâche introuvable"}), 404

    redis_client.delete("all_tasks")
    return jsonify({"id": row[0], "title": row[1], "done": row[2]})

@app.route("/health")
def health():
    """Endpoint de santé pour vérifier que l'app tourne."""
    return jsonify({"status": "ok"})

@app.route("/metrics")
def metrics():
    """Endpoint Prometheus — expose les métriques."""
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

if __name__ == "__main__":
    # On attend un peu que PostgreSQL soit prêt
    time.sleep(3)
    init_db()
    app.run(host="0.0.0.0", port=5000)