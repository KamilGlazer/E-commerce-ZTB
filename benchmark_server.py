"""
Prosty panel HTTP do generowania danych oraz pomiaru czasu odczytów w 4 bazach.
Uruchomienie (z aktywnym venv): uvicorn benchmark_server:app --reload --host 127.0.0.1 --port 8765
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Literal

import pg8000.dbapi
import mysql.connector
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from neo4j import GraphDatabase
from pydantic import BaseModel, Field
from pymongo import MongoClient

from seed import (
    MARIA_CONFIG,
    MONGO_URI,
    NEO4J_PASS,
    NEO4J_URI,
    NEO4J_USER,
    PG_CONFIG,
    SIZES,
    run_seed,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"


SizeName = Literal["small", "medium", "large"]


class SeedRequest(BaseModel):
    size: SizeName = Field(description="Profil wielkości danych jak w seed.py — SIZES")


class RunQueryRequest(BaseModel):
    query_id: str


# --- Zapytania: ten sam zestaw pomysłów, dopasowany do każdego silnika ---
SQL_DEFINITIONS: list[tuple[str, str, str]] = [
    (
        "count_products",
        "Liczba produktów",
        "SELECT COUNT(*) AS c FROM products",
    ),
    (
        "scan_orders",
        "Skan: 50k zamówień (LIMIT)",
        "SELECT id, user_id, status, total_amount FROM orders ORDER BY id LIMIT 50000",
    ),
    (
        "join_orders_users",
        "JOIN: zamówienia + użytkownicy (10k wierszy)",
        """
        SELECT o.id AS order_id, o.status, u.username, u.email
        FROM orders o
        JOIN users u ON o.user_id = u.id
        ORDER BY o.id
        LIMIT 10000
        """,
    ),
    (
        "aggregate_orders_status",
        "Agregacja: zamówienia wg statusu",
        """
        SELECT status, COUNT(*) AS cnt, AVG(total_amount) AS avg_total
        FROM orders
        GROUP BY status
        """,
    ),
    (
        "join_review_product",
        "JOIN: recenzje + produkty (top 5k)",
        """
        SELECT r.id AS review_id, r.rating, p.name AS product_name, p.price
        FROM reviews r
        JOIN products p ON r.product_id = p.id
        ORDER BY r.id
        LIMIT 5000
        """,
    ),
]

MONGO_PIPELINES: dict[str, tuple[str, list[dict[str, Any]]]] = {
    "count_products": (
        "Liczba dokumentów w products",
        [{"$count": "c"}],
    ),
    "scan_orders": (
        "Skan: 50k zamówień",
        [
            {"$project": {"_id": 0, "id": 1, "user_id": 1, "status": 1, "total_amount": 1}},
            {"$sort": {"id": 1}},
            {"$limit": 50000},
        ],
    ),
    "join_orders_users": (
        "$lookup: zamówienia + użytkownicy (10k)",
        [
            {"$sort": {"id": 1}},
            {"$limit": 10000},
            {
                "$lookup": {
                    "from": "users",
                    "localField": "user_id",
                    "foreignField": "id",
                    "as": "u",
                }
            },
            {"$unwind": "$u"},
            {
                "$project": {
                    "_id": 0,
                    "order_id": "$id",
                    "status": 1,
                    "username": "$u.username",
                    "email": "$u.email",
                }
            },
        ],
    ),
    "aggregate_orders_status": (
        "Agregacja: zamówienia wg statusu",
        [
            {
                "$group": {
                    "_id": "$status",
                    "cnt": {"$sum": 1},
                    "avg_total": {"$avg": "$total_amount"},
                }
            },
        ],
    ),
    "join_review_product": (
        "$lookup: recenzje + produkty (5k)",
        [
            {"$sort": {"id": 1}},
            {"$limit": 5000},
            {
                "$lookup": {
                    "from": "products",
                    "localField": "product_id",
                    "foreignField": "id",
                    "as": "p",
                }
            },
            {"$unwind": "$p"},
            {
                "$project": {
                    "_id": 0,
                    "review_id": "$id",
                    "rating": 1,
                    "product_name": "$p.name",
                    "price": "$p.price",
                }
            },
        ],
    ),
}

NEO4J_CYPHER: dict[str, tuple[str, str]] = {
    "count_products": (
        "MATCH (p:Product) RETURN count(p) AS c",
        "Liczba węzłów Product",
    ),
    "scan_orders": (
        "MATCH (o:Order) RETURN o.id AS id, o.user_id AS user_id, o.status AS status, o.total_amount AS total_amount ORDER BY o.id LIMIT 50000",
        "Skan: 50k zamówień",
    ),
    "join_orders_users": (
        """
        MATCH (u:User)-[:PLACED]->(o:Order)
        RETURN o.id AS order_id, o.status AS status, u.username AS username, u.email AS email
        ORDER BY o.id
        LIMIT 10000
        """,
        "Ścieżka: User-PLACED-Order (10k)",
    ),
    "aggregate_orders_status": (
        """
        MATCH (o:Order)
        RETURN o.status AS status, count(o) AS cnt, avg(o.total_amount) AS avg_total
        """,
        "Agregacja zamówień wg statusu",
    ),
    "join_review_product": (
        """
        MATCH (r:Review)-[:REVIEWS]->(p:Product)
        RETURN r.id AS review_id, r.rating AS rating, p.name AS product_name, p.price AS price
        ORDER BY r.id
        LIMIT 5000
        """,
        "Ścieżka: Review-REVIEWS-Product (5k)",
    ),
}


def _meta_queries() -> dict[str, list[dict[str, str]]]:
    sql_opts = [{"id": qid, "label": lab} for qid, lab, _ in SQL_DEFINITIONS]
    mongo_opts = [{"id": k, "label": v[0]} for k, v in MONGO_PIPELINES.items()]
    neo_opts = [{"id": k, "label": v[1]} for k, v in NEO4J_CYPHER.items()]
    return {
        "postgres": sql_opts,
        "mariadb": sql_opts,
        "mongodb": mongo_opts,
        "neo4j": neo_opts,
    }


_SQL_BY_ID = {row[0]: row[2] for row in SQL_DEFINITIONS}


def _timed_rows(fn: Callable[[], Any]) -> tuple[float, int]:
    t0 = time.perf_counter()
    out = fn()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    if isinstance(out, int):
        return elapsed_ms, out
    if isinstance(out, list):
        return elapsed_ms, len(out)
    if out is None:
        return elapsed_ms, 0
    return elapsed_ms, 1


def run_postgres(query_id: str) -> tuple[float, int]:
    sql = _SQL_BY_ID.get(query_id)
    if not sql:
        raise ValueError(f"Nieznane query_id dla SQL: {query_id}")

    def work() -> Any:
        conn = pg8000.dbapi.connect(**PG_CONFIG)
        try:
            cur = conn.cursor()
            cur.execute(sql.strip())
            rows = cur.fetchall()
            cur.close()
            return rows
        finally:
            conn.close()

    return _timed_rows(work)


def run_mariadb(query_id: str) -> tuple[float, int]:
    sql = _SQL_BY_ID.get(query_id)
    if not sql:
        raise ValueError(f"Nieznane query_id dla SQL: {query_id}")

    def work() -> Any:
        conn = mysql.connector.connect(**MARIA_CONFIG)
        try:
            cur = conn.cursor()
            cur.execute(sql.strip())
            rows = cur.fetchall()
            cur.close()
            return rows
        finally:
            conn.close()

    return _timed_rows(work)


def run_mongo(query_id: str) -> tuple[float, int]:
    pipe_entry = MONGO_PIPELINES.get(query_id)
    if not pipe_entry:
        raise ValueError(f"Nieznane query_id dla MongoDB: {query_id}")
    _, pipeline = pipe_entry
    coll = "orders" if query_id != "count_products" else "products"
    if query_id == "join_review_product":
        coll = "reviews"

    def work() -> Any:
        client = MongoClient(MONGO_URI)
        try:
            cur = client["ecommerce"][coll].aggregate(pipeline)
            return list(cur)
        finally:
            client.close()

    return _timed_rows(work)


def run_neo4j(query_id: str) -> tuple[float, int]:
    entry = NEO4J_CYPHER.get(query_id)
    if not entry:
        raise ValueError(f"Nieznane query_id dla Neo4j: {query_id}")
    cypher = entry[0]

    def work() -> Any:
        drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        try:
            with drv.session() as session:
                res = session.run(cypher.strip())
                return list(res)
        finally:
            drv.close()

    return _timed_rows(work)


RUNNERS = {
    "postgres": run_postgres,
    "mariadb": run_mariadb,
    "mongodb": run_mongo,
    "neo4j": run_neo4j,
}

DISPLAY_NAMES = {
    "postgres": "PostgreSQL",
    "mariadb": "MariaDB",
    "mongodb": "MongoDB",
    "neo4j": "Neo4j",
}

app = FastAPI(title="E-commerce DB benchmark")


@app.on_event("startup")
def ensure_static_dir() -> None:
    STATIC_DIR.mkdir(parents=True, exist_ok=True)


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def serve_ui() -> FileResponse:
    index = STATIC_DIR / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=500, detail="Brak static/index.html")
    return FileResponse(index)


def _sizes_meta() -> list[dict[str, Any]]:
    order: list[SizeName] = ["small", "medium", "large"]
    out = []
    for key in order:
        cfg = SIZES[key]
        out.append(
            {
                "id": key,
                "label": f"{key.capitalize()}",
                "products": cfg["products"],
                "orders": cfg["orders"],
            }
        )
    return out


@app.get("/api/meta")
def api_meta() -> dict[str, Any]:
    return {
        "engines": [
            {"id": k, "label": DISPLAY_NAMES[k]}
            for k in ["postgres", "mariadb", "mongodb", "neo4j"]
        ],
        "queries_by_engine": _meta_queries(),
        "sizes": _sizes_meta(),
    }


@app.post("/api/seed")
def api_seed(body: SeedRequest) -> dict[str, Any]:
    cfg = SIZES[body.size]
    try:
        run_seed(cfg)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "ok": True,
        "size": body.size,
        "cfg": cfg,
        "message": f"Dane zostały wygenerowane we wszystkich bazach (profil {body.size}).",
    }


@app.post("/api/run/{engine}")
def api_run(engine: str, body: RunQueryRequest) -> dict[str, Any]:
    if engine not in RUNNERS:
        raise HTTPException(status_code=400, detail=f"Nieznany silnik: {engine}")
    runner = RUNNERS[engine]
    try:
        ms, rows = runner(body.query_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "engine": engine,
        "engine_label": DISPLAY_NAMES[engine],
        "query_id": body.query_id,
        "elapsed_ms": round(ms, 3),
        "row_count": rows,
    }

