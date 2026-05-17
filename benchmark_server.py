"""
Prosty panel HTTP do generowania danych oraz pomiaru czasu odczytów w 4 bazach.
Uruchomienie (z aktywnym venv): uvicorn benchmark_server:app --reload --host 127.0.0.1 --port 8765
"""
from __future__ import annotations

import time
from pathlib import Path
from collections.abc import Sized
from typing import Any, Callable, Literal

import psycopg
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


SCENARIO_LABELS: list[tuple[str, str]] = [
    ("C1", "Create: masowy insert użytkowników"),
    ("C2", "Create: zamówienie + pozycje (transakcja)"),
    ("C3", "Create: recenzje powiązane z user+product"),
    ("C4", "Create: koszyk + pozycje koszyka"),
    ("C5", "Create: płatność + wysyłka"),
    ("C6", "Create: konflikt klucza (obsługa błędu)"),
    ("R1", "Read: lookup produktu po ID"),
    ("R2", "Read: odczyt zamówień po user_id"),
    ("R3", "Read: orders + users + payments"),
    ("R4", "Read: agregacja status/metoda płatności"),
    ("R5", "Read: paginacja produktów (sort + limit)"),
    ("R6", "Read: ścieżka klient -> zamówienie -> produkt"),
    ("U1", "Update: zmiana stock produktów"),
    ("U2", "Update: status PENDING -> SHIPPED"),
    ("U3", "Update: podbicie cen kategorii"),
    ("U4", "Update: modyfikacja recenzji"),
    ("U5", "Update: retry płatności FAILED -> SUCCESS"),
    ("U6", "Update: wielokrotna zmiana statusu jednego zamówienia"),
    ("D1", "Delete: koszyk i pozycje koszyka"),
    ("D2", "Delete: usuwanie recenzji po dacie"),
    ("D3", "Delete: usuwanie zamówień CANCELLED"),
    ("D4", "Delete: usunięcie użytkownika i zależności"),
    ("D5", "Delete: konflikt FK przy usuwaniu produktu"),
    ("D6", "Delete: soft+hard delete produktów"),
]


def _meta_queries() -> dict[str, list[dict[str, str]]]:
    opts = [{"id": sid, "label": f"[{sid}] {label}"} for sid, label in SCENARIO_LABELS]
    return {
        "postgres": opts,
        "mariadb": opts,
        "mongodb": opts,
        "neo4j": opts,
    }


def _execute_timed(query_id: str, work_fn: Callable[[], Any]) -> dict[str, Any]:
    """
    R (Read) - wykonuje warm-up + 50 iteracji (zwraca medianę i p95)
    C/U/D (Create/Update/Delete) - wykonuje 1 iterację (chroni przed błędami spójności)
    """
    is_read = query_id.startswith("R")
    
    def _extract_rows(out_data: Any) -> int:
        if isinstance(out_data, int):
            return out_data
        if out_data is None:
            return 0
        if isinstance(out_data, Sized):
            return len(out_data)
        return 1

    if not is_read:
        # Mutacje stanu wywołujemy TYLKO RAZ
        t0 = time.perf_counter()
        out = work_fn()
        ms = (time.perf_counter() - t0) * 1000.0
        return {
            "median_ms": round(ms, 3),
            "p95_ms": round(ms, 3),
            "iterations": 1,
            "row_count": _extract_rows(out),
        }

    # Warm-up (rozgrzanie cache silnika DB / planów zapytań)
    work_fn()

    # 50 prób dla stabilnego pomiaru
    times = []
    rows = 0
    for i in range(50):
        t0 = time.perf_counter()
        out = work_fn()
        t_ms = (time.perf_counter() - t0) * 1000.0
        times.append(t_ms)
        if i == 0:
            rows = _extract_rows(out)

    times.sort()
    median = times[len(times) // 2]
    p95 = times[int(len(times) * 0.95)]
    
    return {
        "median_ms": round(median, 3),
        "p95_ms": round(p95, 3),
        "iterations": 50,
        "row_count": rows,
    }


def run_postgres(query_id: str) -> dict[str, Any]:
    pg_kwargs = PG_CONFIG.copy()
    pg_kwargs["dbname"] = pg_kwargs.pop("database")
    
    # Otwarte poza timerem, żeby nie wliczać czasu TCP handshake'a
    conn = psycopg.connect(**pg_kwargs)
    try:
        def work() -> Any:
            return _run_sql_crud(conn, query_id, "postgres")
        return _execute_timed(query_id, work)
    finally:
        conn.close()


def run_mariadb(query_id: str) -> dict[str, Any]:
    conn = mysql.connector.connect(**MARIA_CONFIG)
    try:
        def work() -> Any:
            return _run_sql_crud(conn, query_id, "mariadb")
        return _execute_timed(query_id, work)
    finally:
        conn.close()


def run_mongo(query_id: str) -> dict[str, Any]:
    client = MongoClient(MONGO_URI)
    try:
        db = client["ecommerce"]
        def work() -> Any:
            return _run_mongo_crud(db, query_id)
        return _execute_timed(query_id, work)
    finally:
        client.close()


def run_neo4j(query_id: str) -> dict[str, Any]:
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    try:
        def work() -> Any:
            with drv.session() as session:
                return _run_neo4j_crud(session, query_id)
        return _execute_timed(query_id, work)
    finally:
        drv.close()


def _row_count(cur: Any) -> int:
    if cur.rowcount is None or cur.rowcount < 0:
        return 0
    return int(cur.rowcount)


def _next_id(cur: Any, table: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}")
    return int(cur.fetchone()[0])


def _run_sql_crud(conn: Any, query_id: str, engine: str) -> Any:
    cur = conn.cursor()
    try:
        if query_id == "C1":
            start = _next_id(cur, "users")
            batch = [
                (start + i, f"bench_c1_user_{start+i}", f"bench_c1_{start+i}@example.com")
                for i in range(500)
            ]
            cur.executemany(
                "INSERT INTO users (id, username, email) VALUES (%s, %s, %s)",
                batch,
            )
            conn.commit()
            return len(batch)

        if query_id == "C2":
            user_id, product_id = 1, 1
            start_order = _next_id(cur, "orders")
            start_item = _next_id(cur, "order_items")
            orders = [(start_order + i, user_id, "PENDING", 199.99) for i in range(100)]
            cur.executemany(
                "INSERT INTO orders (id, user_id, status, total_amount) VALUES (%s, %s, %s, %s)",
                orders,
            )
            items = []
            for i in range(100):
                oid = start_order + i
                for j in range(5):
                    items.append((start_item + (i * 5) + j, oid, product_id, 1, 39.99))
            cur.executemany(
                "INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s, %s)",
                items,
            )
            conn.commit()
            return len(orders) + len(items)

        if query_id == "C3":
            start = _next_id(cur, "reviews")
            batch = [(start + i, 1, 1, 5, "bench review") for i in range(500)]
            cur.executemany(
                "INSERT INTO reviews (id, product_id, user_id, rating, comment) VALUES (%s, %s, %s, %s, %s)",
                batch,
            )
            conn.commit()
            return len(batch)

        if query_id == "C4":
            start_cart = _next_id(cur, "carts")
            start_item = _next_id(cur, "cart_items")
            carts = [(start_cart + i, 1) for i in range(100)]
            cur.executemany("INSERT INTO carts (id, user_id) VALUES (%s, %s)", carts)
            items = []
            for i in range(100):
                cid = start_cart + i
                for j in range(5):
                    items.append((start_item + (i * 5) + j, cid, 1, 1))
            cur.executemany(
                "INSERT INTO cart_items (id, cart_id, product_id, quantity) VALUES (%s, %s, %s, %s)",
                items,
            )
            conn.commit()
            return len(carts) + len(items)

        if query_id == "C5":
            start_pay = _next_id(cur, "payments")
            start_ship = _next_id(cur, "shipments")
            payments = [(start_pay + i, 1, 120.00, "BLIK", "SUCCESS") for i in range(200)]
            shipments = [
                (start_ship + i, 1, f"BENCHTRK{start_ship+i:08d}", "INPOST", "IN_TRANSIT")
                for i in range(200)
            ]
            cur.executemany(
                "INSERT INTO payments (id, order_id, amount, method, status) VALUES (%s, %s, %s, %s, %s)",
                payments,
            )
            cur.executemany(
                "INSERT INTO shipments (id, order_id, tracking_number, carrier, status) VALUES (%s, %s, %s, %s, %s)",
                shipments,
            )
            conn.commit()
            return len(payments) + len(shipments)

        if query_id == "C6":
            dup_id = _next_id(cur, "users")
            cur.execute(
                "INSERT INTO users (id, username, email) VALUES (%s, %s, %s)",
                (dup_id, "bench_dup", f"bench_dup_{dup_id}@example.com"),
            )
            conn.commit()
            try:
                cur.execute(
                    "INSERT INTO users (id, username, email) VALUES (%s, %s, %s)",
                    (dup_id, "bench_dup2", f"bench_dup2_{dup_id}@example.com"),
                )
                conn.commit()
                return 0
            except Exception:
                conn.rollback()
                return 1

        if query_id == "R1":
            cur.execute("SELECT id, name, price FROM products ORDER BY id LIMIT 1000")
            return cur.fetchall()
        if query_id == "R2":
            cur.execute("SELECT id, user_id, status FROM orders WHERE user_id = 1 ORDER BY id LIMIT 5000")
            return cur.fetchall()
        if query_id == "R3":
            cur.execute(
                """
                SELECT o.id, u.username, p.status
                FROM orders o
                JOIN users u ON u.id = o.user_id
                JOIN payments p ON p.order_id = o.id
                ORDER BY o.id
                LIMIT 5000
                """
            )
            return cur.fetchall()
        if query_id == "R4":
            cur.execute(
                """
                SELECT o.status, p.method, COUNT(*) AS cnt, AVG(p.amount) AS avg_amount
                FROM orders o
                JOIN payments p ON p.order_id = o.id
                GROUP BY o.status, p.method
                """
            )
            return cur.fetchall()
        if query_id == "R5":
            cur.execute("SELECT id, name, price FROM products ORDER BY price, id LIMIT 2000")
            return cur.fetchall()
        if query_id == "R6":
            cur.execute(
                """
                SELECT u.id, o.id, oi.product_id
                FROM users u
                JOIN orders o ON o.user_id = u.id
                JOIN order_items oi ON oi.order_id = o.id
                ORDER BY u.id, o.id
                LIMIT 5000
                """
            )
            return cur.fetchall()

        if query_id == "U1":
            cur.execute("UPDATE products SET stock = CASE WHEN stock > 0 THEN stock - 1 ELSE 0 END WHERE id <= 2000")
            conn.commit()
            return _row_count(cur)
        if query_id == "U2":
            cur.execute("UPDATE orders SET status = 'SHIPPED' WHERE status = 'PENDING'")
            conn.commit()
            return _row_count(cur)
        if query_id == "U3":
            cur.execute("UPDATE products SET price = ROUND(price * 1.05, 2) WHERE category_id = 1")
            conn.commit()
            return _row_count(cur)
        if query_id == "U4":
            cur.execute("UPDATE reviews SET rating = 4, comment = 'bench update' WHERE user_id = 1 AND product_id = 1")
            conn.commit()
            return _row_count(cur)
        if query_id == "U5":
            cur.execute("UPDATE payments SET status = 'SUCCESS' WHERE status = 'FAILED'")
            conn.commit()
            return _row_count(cur)
        if query_id == "U6":
            cur.execute("UPDATE orders SET status = 'PENDING' WHERE id = 1")
            cur.execute("UPDATE orders SET status = 'SHIPPED' WHERE id = 1")
            cur.execute("UPDATE orders SET status = 'COMPLETED' WHERE id = 1")
            conn.commit()
            return 3

        if query_id == "D1":
            cur.execute("DELETE FROM cart_items WHERE cart_id IN (SELECT id FROM carts WHERE id <= 50)")
            deleted_items = _row_count(cur)
            cur.execute("DELETE FROM carts WHERE id <= 50")
            deleted_carts = _row_count(cur)
            conn.commit()
            return deleted_items + deleted_carts
        if query_id == "D2":
            cur.execute("DELETE FROM reviews WHERE created_at < '2024-01-09'")
            conn.commit()
            return _row_count(cur)
        if query_id == "D3":
            cur.execute("SELECT id FROM orders WHERE status = 'CANCELLED' ORDER BY id LIMIT 200")
            ids = [row[0] for row in cur.fetchall()]
            if not ids:
                return 0
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(f"DELETE FROM order_items WHERE order_id IN ({placeholders})", tuple(ids))
            c1 = _row_count(cur)
            cur.execute(f"DELETE FROM payments WHERE order_id IN ({placeholders})", tuple(ids))
            c2 = _row_count(cur)
            cur.execute(f"DELETE FROM shipments WHERE order_id IN ({placeholders})", tuple(ids))
            c3 = _row_count(cur)
            cur.execute(f"DELETE FROM orders WHERE id IN ({placeholders})", tuple(ids))
            c4 = _row_count(cur)
            conn.commit()
            return c1 + c2 + c3 + c4
        if query_id == "D4":
            user_id = 2
            cur.execute("SELECT id FROM orders WHERE user_id = %s LIMIT 100", (user_id,))
            ids = [row[0] for row in cur.fetchall()]
            deleted = 0
            if ids:
                placeholders = ",".join(["%s"] * len(ids))
                cur.execute(f"DELETE FROM order_items WHERE order_id IN ({placeholders})", tuple(ids))
                deleted += _row_count(cur)
                cur.execute(f"DELETE FROM payments WHERE order_id IN ({placeholders})", tuple(ids))
                deleted += _row_count(cur)
                cur.execute(f"DELETE FROM shipments WHERE order_id IN ({placeholders})", tuple(ids))
                deleted += _row_count(cur)
                cur.execute(f"DELETE FROM orders WHERE id IN ({placeholders})", tuple(ids))
                deleted += _row_count(cur)
            cur.execute("DELETE FROM reviews WHERE user_id = %s", (user_id,))
            deleted += _row_count(cur)
            cur.execute("DELETE FROM cart_items WHERE cart_id IN (SELECT id FROM carts WHERE user_id = %s)", (user_id,))
            deleted += _row_count(cur)
            cur.execute("DELETE FROM carts WHERE user_id = %s", (user_id,))
            deleted += _row_count(cur)
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
            deleted += _row_count(cur)
            conn.commit()
            return deleted
        if query_id == "D5":
            try:
                cur.execute("DELETE FROM products WHERE id = 1")
                conn.commit()
                return _row_count(cur)
            except Exception:
                conn.rollback()
                return 1
        if query_id == "D6":
            if engine == "postgres":
                cur.execute("UPDATE products SET name = 'SOFT_' || name WHERE id BETWEEN 10 AND 200")
            else:
                cur.execute("UPDATE products SET name = CONCAT('SOFT_', name) WHERE id BETWEEN 10 AND 200")
            soft = _row_count(cur)
            cur.execute(
                """
                DELETE FROM products
                WHERE id BETWEEN 10 AND 50
                  AND id NOT IN (
                      SELECT DISTINCT product_id
                      FROM order_items
                      WHERE product_id IS NOT NULL
                  )
                """
            )
            hard = _row_count(cur)
            conn.commit()
            return soft + hard

        raise ValueError(f"Nieznane query_id: {query_id}")
    finally:
        cur.close()


def _run_mongo_crud(db: Any, query_id: str) -> Any:
    if query_id == "C1":
        start = (db.users.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        docs = [{"id": start + i, "username": f"bench_c1_user_{start+i}", "email": f"bench_c1_{start+i}@example.com"} for i in range(500)]
        return db.users.insert_many(docs, ordered=False).inserted_ids and len(docs)
    if query_id == "C2":
        start_o = (db.orders.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        start_i = (db.order_items.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        orders = [{"id": start_o + i, "user_id": 1, "status": "PENDING", "total_amount": 199.99} for i in range(100)]
        db.orders.insert_many(orders, ordered=False)
        items = []
        for i in range(100):
            oid = start_o + i
            for j in range(5):
                items.append({"id": start_i + (i * 5) + j, "order_id": oid, "product_id": 1, "quantity": 1, "unit_price": 39.99})
        db.order_items.insert_many(items, ordered=False)
        return len(orders) + len(items)
    if query_id == "C3":
        start = (db.reviews.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        docs = [{"id": start + i, "product_id": 1, "user_id": 1, "rating": 5, "comment": "bench review"} for i in range(500)]
        db.reviews.insert_many(docs, ordered=False)
        return len(docs)
    if query_id == "C4":
        start_c = (db.carts.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        start_i = (db.cart_items.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        carts = [{"id": start_c + i, "user_id": 1} for i in range(100)]
        db.carts.insert_many(carts, ordered=False)
        items = []
        for i in range(100):
            cid = start_c + i
            for j in range(5):
                items.append({"id": start_i + (i * 5) + j, "cart_id": cid, "product_id": 1, "quantity": 1})
        db.cart_items.insert_many(items, ordered=False)
        return len(carts) + len(items)
    if query_id == "C5":
        start_p = (db.payments.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        start_s = (db.shipments.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        pays = [{"id": start_p + i, "order_id": 1, "amount": 120.0, "method": "BLIK", "status": "SUCCESS"} for i in range(200)]
        ships = [{"id": start_s + i, "order_id": 1, "tracking_number": f"BENCHTRK{start_s+i:08d}", "carrier": "INPOST", "status": "IN_TRANSIT"} for i in range(200)]
        db.payments.insert_many(pays, ordered=False)
        db.shipments.insert_many(ships, ordered=False)
        return len(pays) + len(ships)
    if query_id == "C6":
        start = (db.users.find_one(sort=[("id", -1)]) or {}).get("id", 0) + 1
        db.users.insert_one({"id": start, "username": "bench_dup", "email": f"bench_dup_{start}@example.com"})
        db.users.create_index("id", unique=True)
        try:
            db.users.insert_one({"id": start, "username": "bench_dup2", "email": f"bench_dup2_{start}@example.com"})
            return 0
        except Exception:
            return 1
    if query_id == "R1":
        return list(db.products.find({}, {"_id": 0, "id": 1, "name": 1, "price": 1}).sort("id", 1).limit(1000))
    if query_id == "R2":
        return list(db.orders.find({"user_id": 1}, {"_id": 0, "id": 1, "status": 1}).sort("id", 1).limit(5000))
    if query_id == "R3":
        return list(db.orders.aggregate([
            {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "id", "as": "u"}},
            {"$lookup": {"from": "payments", "localField": "id", "foreignField": "order_id", "as": "p"}},
            {"$unwind": "$u"},
            {"$unwind": "$p"},
            {"$project": {"_id": 0, "order_id": "$id", "username": "$u.username", "payment_status": "$p.status"}},
            {"$limit": 5000},
        ]))
    if query_id == "R4":
        return list(db.payments.aggregate([
            {"$group": {"_id": {"status": "$status", "method": "$method"}, "cnt": {"$sum": 1}, "avg_amount": {"$avg": "$amount"}}}
        ]))
    if query_id == "R5":
        return list(db.products.find({}, {"_id": 0, "id": 1, "name": 1, "price": 1}).sort([("price", 1), ("id", 1)]).limit(2000))
    if query_id == "R6":
        return list(db.orders.aggregate([
            {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "id", "as": "u"}},
            {"$lookup": {"from": "order_items", "localField": "id", "foreignField": "order_id", "as": "oi"}},
            {"$unwind": "$u"},
            {"$unwind": "$oi"},
            {"$project": {"_id": 0, "user_id": "$u.id", "order_id": "$id", "product_id": "$oi.product_id"}},
            {"$limit": 5000},
        ]))
    if query_id == "U1":
        res = db.products.update_many({"id": {"$lte": 2000}}, [{"$set": {"stock": {"$max": [0, {"$subtract": ["$stock", 1]}]}}}])
        return int(res.modified_count)
    if query_id == "U2":
        return int(db.orders.update_many({"status": "PENDING"}, {"$set": {"status": "SHIPPED"}}).modified_count)
    if query_id == "U3":
        res = db.products.update_many({"category_id": 1}, [{"$set": {"price": {"$round": [{"$multiply": ["$price", 1.05]}, 2]}}}])
        return int(res.modified_count)
    if query_id == "U4":
        return int(db.reviews.update_many({"user_id": 1, "product_id": 1}, {"$set": {"rating": 4, "comment": "bench update"}}).modified_count)
    if query_id == "U5":
        return int(db.payments.update_many({"status": "FAILED"}, {"$set": {"status": "SUCCESS"}}).modified_count)
    if query_id == "U6":
        db.orders.update_one({"id": 1}, {"$set": {"status": "PENDING"}})
        db.orders.update_one({"id": 1}, {"$set": {"status": "SHIPPED"}})
        db.orders.update_one({"id": 1}, {"$set": {"status": "COMPLETED"}})
        return 3
    if query_id == "D1":
        carts = [d["id"] for d in db.carts.find({"id": {"$lte": 50}}, {"_id": 0, "id": 1})]
        a = db.cart_items.delete_many({"cart_id": {"$in": carts}}).deleted_count
        b = db.carts.delete_many({"id": {"$in": carts}}).deleted_count
        return int(a + b)
    if query_id == "D2":
        return int(db.reviews.delete_many({"created_at": {"$lt": "2024-01-09"}}).deleted_count)
    if query_id == "D3":
        ids = [d["id"] for d in db.orders.find({"status": "CANCELLED"}, {"_id": 0, "id": 1}).sort("id", 1).limit(200)]
        a = db.order_items.delete_many({"order_id": {"$in": ids}}).deleted_count
        b = db.payments.delete_many({"order_id": {"$in": ids}}).deleted_count
        c = db.shipments.delete_many({"order_id": {"$in": ids}}).deleted_count
        d = db.orders.delete_many({"id": {"$in": ids}}).deleted_count
        return int(a + b + c + d)
    if query_id == "D4":
        user_id = 2
        ids = [d["id"] for d in db.orders.find({"user_id": user_id}, {"_id": 0, "id": 1}).limit(100)]
        deleted = 0
        deleted += db.order_items.delete_many({"order_id": {"$in": ids}}).deleted_count
        deleted += db.payments.delete_many({"order_id": {"$in": ids}}).deleted_count
        deleted += db.shipments.delete_many({"order_id": {"$in": ids}}).deleted_count
        deleted += db.orders.delete_many({"id": {"$in": ids}}).deleted_count
        deleted += db.reviews.delete_many({"user_id": user_id}).deleted_count
        carts = [d["id"] for d in db.carts.find({"user_id": user_id}, {"_id": 0, "id": 1})]
        deleted += db.cart_items.delete_many({"cart_id": {"$in": carts}}).deleted_count
        deleted += db.carts.delete_many({"user_id": user_id}).deleted_count
        deleted += db.users.delete_many({"id": user_id}).deleted_count
        return int(deleted)
    if query_id == "D5":
        has_refs = db.order_items.count_documents({"product_id": 1}) > 0
        if has_refs:
            return 1
        return int(db.products.delete_one({"id": 1}).deleted_count)
    if query_id == "D6":
        soft = db.products.update_many({"id": {"$gte": 10, "$lte": 200}}, {"$set": {"soft_deleted": True}}).modified_count
        hard = db.products.delete_many({"id": {"$gte": 10, "$lte": 50}}).deleted_count
        return int(soft + hard)
    raise ValueError(f"Nieznane query_id dla MongoDB: {query_id}")


def _run_neo4j_crud(session: Any, query_id: str) -> Any:
    if query_id == "C1":
        return int(session.run("UNWIND range(1,500) AS i CREATE (:User {id: 100000000 + i, username: 'bench_c1_'+toString(i), email: 'bench_c1_'+toString(i)+'@example.com'}) RETURN count(*) AS c").single()["c"])
    if query_id == "C2":
        return int(session.run("UNWIND range(1,100) AS i MATCH (u:User {id: 1}) CREATE (o:Order {id: 200000000 + i, user_id: 1, status: 'PENDING', total_amount: 199.99}) CREATE (u)-[:PLACED]->(o) WITH o,i UNWIND range(1,5) AS j CREATE (oi:OrderItem {id: 300000000 + (i*10) + j, order_id: o.id, product_id: 1, quantity: 1, unit_price: 39.99}) CREATE (o)-[:HAS_ITEM]->(oi) RETURN count(*) AS c").single()["c"])
    if query_id == "C3":
        return int(session.run("UNWIND range(1,500) AS i MATCH (u:User {id: 1}), (p:Product {id: 1}) CREATE (r:Review {id: 400000000 + i, user_id: 1, product_id: 1, rating: 5, comment: 'bench review'}) CREATE (u)-[:WROTE_REVIEW]->(r) CREATE (r)-[:REVIEWS]->(p) RETURN count(r) AS c").single()["c"])
    if query_id == "C4":
        return int(session.run("UNWIND range(1,100) AS i MATCH (u:User {id:1}) CREATE (c:Cart {id: 500000000 + i, user_id: 1}) CREATE (u)-[:HAS_CART]->(c) WITH c,i UNWIND range(1,5) AS j MATCH (p:Product {id:1}) CREATE (ci:CartItem {id: 600000000 + (i*10) + j, cart_id: c.id, product_id: 1, quantity: 1}) CREATE (c)-[:CONTAINS]->(ci) CREATE (ci)-[:FOR_PRODUCT]->(p) RETURN count(ci) AS c").single()["c"])
    if query_id == "C5":
        return int(session.run("UNWIND range(1,200) AS i MATCH (o:Order {id:1}) CREATE (:Payment {id: 700000000 + i, order_id: 1, amount: 120.0, method: 'BLIK', status: 'SUCCESS'})-[:PAYS_FOR]->(o) CREATE (:Shipment {id: 800000000 + i, order_id: 1, tracking_number: 'BENCHTRK'+toString(800000000+i), carrier: 'INPOST', status: 'IN_TRANSIT'})-[:SHIPS]->(o) RETURN count(*) AS c").single()["c"])
    if query_id == "C6":
        session.run("MERGE (u:User {id: 900000001}) SET u.username = 'dup', u.email = 'dup@example.com'")
        try:
            session.run("CREATE (:User {id: 900000001, username: 'dup2', email: 'dup2@example.com'})")
            return 0
        except Exception:
            return 1
    if query_id == "R1":
        return list(session.run("MATCH (p:Product) RETURN p.id AS id, p.name AS name, p.price AS price ORDER BY p.id LIMIT 1000"))
    if query_id == "R2":
        return list(session.run("MATCH (o:Order {user_id: 1}) RETURN o.id AS id, o.status AS status ORDER BY o.id LIMIT 5000"))
    if query_id == "R3":
        return list(session.run("MATCH (u:User)-[:PLACED]->(o:Order)<-[:PAYS_FOR]-(p:Payment) RETURN o.id AS order_id, u.username AS username, p.status AS payment_status ORDER BY o.id LIMIT 5000"))
    if query_id == "R4":
        return list(session.run("MATCH (p:Payment) RETURN p.status AS status, p.method AS method, count(*) AS cnt, avg(p.amount) AS avg_amount"))
    if query_id == "R5":
        return list(session.run("MATCH (p:Product) RETURN p.id AS id, p.name AS name, p.price AS price ORDER BY p.price, p.id LIMIT 2000"))
    if query_id == "R6":
        return list(session.run("MATCH (u:User)-[:PLACED]->(o:Order)-[:HAS_ITEM]->(oi:OrderItem)-[:FOR_PRODUCT]->(p:Product) RETURN u.id AS user_id, o.id AS order_id, p.id AS product_id ORDER BY u.id, o.id LIMIT 5000"))
    if query_id == "U1":
        return int(session.run("MATCH (p:Product) WHERE p.id <= 2000 SET p.stock = CASE WHEN p.stock > 0 THEN p.stock - 1 ELSE 0 END RETURN count(p) AS c").single()["c"])
    if query_id == "U2":
        return int(session.run("MATCH (o:Order {status:'PENDING'}) SET o.status = 'SHIPPED' RETURN count(o) AS c").single()["c"])
    if query_id == "U3":
        return int(session.run("MATCH (p:Product {category_id:1}) SET p.price = round(p.price * 1.05 * 100) / 100 RETURN count(p) AS c").single()["c"])
    if query_id == "U4":
        return int(session.run("MATCH (r:Review {user_id:1, product_id:1}) SET r.rating = 4, r.comment = 'bench update' RETURN count(r) AS c").single()["c"])
    if query_id == "U5":
        return int(session.run("MATCH (p:Payment {status:'FAILED'}) SET p.status = 'SUCCESS' RETURN count(p) AS c").single()["c"])
    if query_id == "U6":
        return int(session.run("MATCH (o:Order {id:1}) SET o.status='PENDING' SET o.status='SHIPPED' SET o.status='COMPLETED' RETURN count(o) AS c").single()["c"])
    if query_id == "D1":
        return int(session.run("MATCH (c:Cart) WHERE c.id <= 50 OPTIONAL MATCH (c)-[:CONTAINS]->(ci:CartItem) DETACH DELETE ci, c RETURN count(*) AS c").single()["c"])
    if query_id == "D2":
        return int(session.run("MATCH (r:Review) WHERE r.created_at < '2024-01-09' WITH collect(r) AS rs FOREACH (x IN rs | DETACH DELETE x) RETURN size(rs) AS c").single()["c"])
    if query_id == "D3":
        return int(session.run("MATCH (o:Order {status:'CANCELLED'}) WITH o LIMIT 200 OPTIONAL MATCH (o)-[:HAS_ITEM]->(oi:OrderItem) OPTIONAL MATCH (p:Payment)-[:PAYS_FOR]->(o) OPTIONAL MATCH (s:Shipment)-[:SHIPS]->(o) DETACH DELETE oi, p, s, o RETURN count(*) AS c").single()["c"])
    if query_id == "D4":
        return int(session.run("MATCH (u:User {id:2}) OPTIONAL MATCH (u)-[:PLACED]->(o:Order) OPTIONAL MATCH (o)-[:HAS_ITEM]->(oi:OrderItem) OPTIONAL MATCH (pay:Payment)-[:PAYS_FOR]->(o) OPTIONAL MATCH (ship:Shipment)-[:SHIPS]->(o) OPTIONAL MATCH (u)-[:WROTE_REVIEW]->(r:Review) OPTIONAL MATCH (u)-[:HAS_CART]->(c:Cart) OPTIONAL MATCH (c)-[:CONTAINS]->(ci:CartItem) DETACH DELETE oi, pay, ship, o, r, ci, c, u RETURN count(*) AS c").single()["c"])
    if query_id == "D5":
        has_ref = session.run("MATCH (oi:OrderItem {product_id:1}) RETURN count(oi) AS c").single()["c"]
        if has_ref > 0:
            return 1
        return int(session.run("MATCH (p:Product {id:1}) DETACH DELETE p RETURN count(p) AS c").single()["c"])
    if query_id == "D6":
        soft = session.run("MATCH (p:Product) WHERE p.id >= 10 AND p.id <= 200 SET p.soft_deleted = true RETURN count(p) AS c").single()["c"]
        hard = session.run("MATCH (p:Product) WHERE p.id >= 10 AND p.id <= 50 DETACH DELETE p RETURN count(p) AS c").single()["c"]
        return int(soft + hard)
    raise ValueError(f"Nieznane query_id dla Neo4j: {query_id}")


_BENCH_SQL_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS bench_idx_orders_user_id ON orders(user_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_orders_status ON orders(status)",
    "CREATE INDEX IF NOT EXISTS bench_idx_products_category_id ON products(category_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_products_price_id ON products(price, id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_order_items_order_id ON order_items(order_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_order_items_product_id ON order_items(product_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_payments_order_id ON payments(order_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_payments_status ON payments(status)",
    "CREATE INDEX IF NOT EXISTS bench_idx_payments_status_method ON payments(status, method)",
    "CREATE INDEX IF NOT EXISTS bench_idx_shipments_order_id ON shipments(order_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_reviews_created_at ON reviews(created_at)",
    "CREATE INDEX IF NOT EXISTS bench_idx_reviews_user_product ON reviews(user_id, product_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_cart_items_cart_id ON cart_items(cart_id)",
    "CREATE INDEX IF NOT EXISTS bench_idx_carts_user_id ON carts(user_id)",
]


_BENCH_MONGO_INDEX_SPECS: list[tuple[str, str, list[tuple[str, int]]]] = [
    ("orders", "bench_orders_user_id", [("user_id", 1)]),
    ("orders", "bench_orders_status", [("status", 1)]),
    ("products", "bench_products_category_id", [("category_id", 1)]),
    ("products", "bench_products_price_id", [("price", 1), ("id", 1)]),
    ("order_items", "bench_order_items_order_id", [("order_id", 1)]),
    ("order_items", "bench_order_items_product_id", [("product_id", 1)]),
    ("payments", "bench_payments_order_id", [("order_id", 1)]),
    ("payments", "bench_payments_status", [("status", 1)]),
    ("payments", "bench_payments_status_method", [("status", 1), ("method", 1)]),
    ("shipments", "bench_shipments_order_id", [("order_id", 1)]),
    ("reviews", "bench_reviews_created_at", [("created_at", 1)]),
    ("reviews", "bench_reviews_user_product", [("user_id", 1), ("product_id", 1)]),
    ("cart_items", "bench_cart_items_cart_id", [("cart_id", 1)]),
    ("carts", "bench_carts_user_id", [("user_id", 1)]),
]

_BENCH_NEO4J_INDEXES: list[str] = [
    "CREATE INDEX bench_order_user_id IF NOT EXISTS FOR (o:Order) ON (o.user_id)",
    "CREATE INDEX bench_order_status IF NOT EXISTS FOR (o:Order) ON (o.status)",
    "CREATE INDEX bench_product_category_id IF NOT EXISTS FOR (p:Product) ON (p.category_id)",
    "CREATE INDEX bench_product_price_id IF NOT EXISTS FOR (p:Product) ON (p.price, p.id)",
    "CREATE INDEX bench_order_item_order_id IF NOT EXISTS FOR (oi:OrderItem) ON (oi.order_id)",
    "CREATE INDEX bench_order_item_product_id IF NOT EXISTS FOR (oi:OrderItem) ON (oi.product_id)",
    "CREATE INDEX bench_payment_order_id IF NOT EXISTS FOR (p:Payment) ON (p.order_id)",
    "CREATE INDEX bench_payment_status IF NOT EXISTS FOR (p:Payment) ON (p.status)",
    "CREATE INDEX bench_payment_status_method IF NOT EXISTS FOR (p:Payment) ON (p.status, p.method)",
    "CREATE INDEX bench_shipment_order_id IF NOT EXISTS FOR (s:Shipment) ON (s.order_id)",
    "CREATE INDEX bench_review_created_at IF NOT EXISTS FOR (r:Review) ON (r.created_at)",
    "CREATE INDEX bench_review_user_product IF NOT EXISTS FOR (r:Review) ON (r.user_id, r.product_id)",
    "CREATE INDEX bench_cart_item_cart_id IF NOT EXISTS FOR (ci:CartItem) ON (ci.cart_id)",
    "CREATE INDEX bench_cart_user_id IF NOT EXISTS FOR (c:Cart) ON (c.user_id)",
]


def _apply_sql_indexes(conn: Any) -> int:
    cur = conn.cursor()
    try:
        for ddl in _BENCH_SQL_INDEXES:
            cur.execute(ddl)
        conn.commit()
        return len(_BENCH_SQL_INDEXES)
    finally:
        cur.close()


def create_postgres_indexes() -> int:
    pg_kwargs = PG_CONFIG.copy()
    pg_kwargs["dbname"] = pg_kwargs.pop("database")
    conn = psycopg.connect(**pg_kwargs)
    try:
        return _apply_sql_indexes(conn)
    finally:
        conn.close()


def create_mariadb_indexes() -> int:
    conn = mysql.connector.connect(**MARIA_CONFIG)
    try:
        return _apply_sql_indexes(conn)
    finally:
        conn.close()


def create_mongo_indexes() -> int:
    client = MongoClient(MONGO_URI)
    try:
        db = client["ecommerce"]
        for coll_name, index_name, key in _BENCH_MONGO_INDEX_SPECS:
            db[coll_name].create_index(key, name=index_name)
        return len(_BENCH_MONGO_INDEX_SPECS)
    finally:
        client.close()


def create_neo4j_indexes() -> int:
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    try:
        with drv.session() as session:
            for cypher in _BENCH_NEO4J_INDEXES:
                session.run(cypher)
            session.run("CALL db.awaitIndexes()") # Czekamy aż Neo4j asynchronicznie skończy
        return len(_BENCH_NEO4J_INDEXES)
    finally:
        drv.close()


INDEX_CREATORS = {
    "postgres": create_postgres_indexes,
    "mariadb": create_mariadb_indexes,
    "mongodb": create_mongo_indexes,
    "neo4j": create_neo4j_indexes,
}


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
        "scenarios": [{"id": sid, "label": label} for sid, label in SCENARIO_LABELS],
        "sizes": _sizes_meta(),
    }


@app.post("/api/indexes")
def api_create_indexes() -> dict[str, Any]:
    results: dict[str, Any] = {}
    for engine, fn in INDEX_CREATORS.items():
        try:
            count = fn()
            results[engine] = {"ok": True, "indexes": count}
        except Exception as exc:
            results[engine] = {"ok": False, "error": str(exc)}
    all_ok = all(r.get("ok") for r in results.values())
    total = sum(r.get("indexes", 0) for r in results.values() if r.get("ok"))
    if all_ok:
        message = (
            f"Indeksy benchmarkowe założone we wszystkich bazach "
            f"(łącznie {total} definicji). Uruchom ponownie scenariusze, aby porównać czasy."
        )
    else:
        failed = [DISPLAY_NAMES[e] for e, r in results.items() if not r.get("ok")]
        message = f"Błąd w: {', '.join(failed)}. Sprawdź, czy kontenery DB działają."
    return {"ok": all_ok, "results": results, "message": message}


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
        res = runner(body.query_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "engine": engine,
        "query_id": body.query_id,
        "median_ms": res["median_ms"],
        "p95_ms": res["p95_ms"],
        "iterations": res["iterations"],
        "row_count": res["row_count"],
    }