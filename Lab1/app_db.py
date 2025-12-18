from flask import Flask, jsonify
import threading
import os

import psycopg2
from psycopg2.pool import SimpleConnectionPool

app = Flask(__name__)

# Параметри підключення до Postgres (заміни на свої)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "webcounter")
DB_USER = os.getenv("DB_USER", "webuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")

# Пул з'єднань до БД (потоко-безпечний)
db_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=20,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)


db_lock = threading.Lock() 


def get_conn():
    return db_pool.getconn()


def put_conn(conn):
    db_pool.putconn(conn)


@app.route("/inc", methods=["GET"])
def inc_db():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:

                cur.execute("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value;")
                (value,) = cur.fetchone()

    finally:
        put_conn(conn)

    return jsonify({"count": value})


@app.route("/count", methods=["GET"])
def count_db():
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM counter WHERE id = 1;")
                (value,) = cur.fetchone()
    finally:
        put_conn(conn)
    return jsonify({"count": value})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
