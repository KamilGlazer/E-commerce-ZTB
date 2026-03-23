import argparse
import random
from tqdm import tqdm
import pg8000.dbapi
import mysql.connector
from pymongo import MongoClient
from neo4j import GraphDatabase

# Konfiguracje połączeń
PG_CONFIG = {"host": "localhost", "port": 5432, "user": "ecom_user", "password": "ecom_password", "database": "ecommerce"}
MARIA_CONFIG = {"host": "127.0.0.1", "port": 3306, "user": "ecom_user", "password": "ecom_password", "database": "ecommerce"}
MONGO_URI = "mongodb://ecom_user:ecom_password@localhost:27017/"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "ecom_password"

CHUNK_SIZE = 10000

# Profile wielkości danych
SIZES = {
    "small": {
        "users": 40000, "categories": 500, "products": 10000, "orders": 80000,
        "order_items": 150000, "reviews": 30000, "carts": 20000, "cart_items": 50000,
        "payments": 60000, "shipments": 59500
    },
    "medium": {
        "users": 80000, "categories": 1000, "products": 20000, "orders": 160000,
        "order_items": 300000, "reviews": 60000, "carts": 40000, "cart_items": 100000,
        "payments": 119000, "shipments": 121000
    },
    "large": {
        "users": 800000, "categories": 10000, "products": 200000, "orders": 1600000,
        "order_items": 3000000, "reviews": 600000, "carts": 400000, "cart_items": 1000000,
        "payments": 1195000, "shipments": 1205000
    }
}

def get_db_connections():
    print("Łączenie z bazami danych...")
    pg_conn = pg8000.dbapi.connect(**PG_CONFIG)
    maria_conn = mysql.connector.connect(**MARIA_CONFIG)
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client["ecommerce"]
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    return pg_conn, maria_conn, mongo_db, neo4j_driver

# Szybkie generatory (zoptymalizowane pod wydajność, omijające bibliotekę Faker)
def gen_users(start, end):
    return [(i, f"user_{i}", f"user_{i}@example.com", "2024-01-01 10:00:00") for i in range(start, end)]

def gen_categories(start, end):
    return [(i, f"Category_{i}", f"Description for category {i}") for i in range(start, end)]

def gen_products(start, end, max_cat):
    return [(i, random.randint(1, max_cat), f"Product_{i}", round(random.uniform(5.0, 500.0), 2), random.randint(0, 1000)) for i in range(start, end)]

def gen_orders(start, end, max_user):
    statuses = ['PENDING', 'SHIPPED', 'COMPLETED', 'CANCELLED']
    return [(i, random.randint(1, max_user), statuses[i % 4], round(random.uniform(20.0, 1000.0), 2), "2024-01-05 12:00:00") for i in range(start, end)]

def gen_order_items(start, end, max_order, max_product):
    return [(i, random.randint(1, max_order), random.randint(1, max_product), random.randint(1, 5), round(random.uniform(5.0, 500.0), 2)) for i in range(start, end)]

def gen_reviews(start, end, max_product, max_user):
    return [(i, random.randint(1, max_product), random.randint(1, max_user), random.randint(1, 5), "Standard review text.", "2024-01-10 15:00:00") for i in range(start, end)]

def gen_carts(start, end, max_user):
    return [(i, random.randint(1, max_user), "2024-01-15 08:00:00") for i in range(start, end)]

def gen_cart_items(start, end, max_cart, max_product):
    return [(i, random.randint(1, max_cart), random.randint(1, max_product), random.randint(1, 10)) for i in range(start, end)]

def gen_payments(start, end, max_order):
    methods = ['CREDIT_CARD', 'PAYPAL', 'BLIK', 'TRANSFER']
    statuses = ['SUCCESS', 'FAILED', 'PENDING']
    return [(i, random.randint(1, max_order), round(random.uniform(20.0, 1000.0), 2), methods[i % 4], statuses[i % 3], "2024-01-06 12:00:00") for i in range(start, end)]

def gen_shipments(start, end, max_order):
    carriers = ['DPD', 'INPOST', 'DHL', 'UPS']
    statuses = ['IN_TRANSIT', 'DELIVERED', 'RETURNED']
    return [(i, random.randint(1, max_order), f"TRK{i:010d}", carriers[i % 4], statuses[i % 3]) for i in range(start, end)]

# Funkcje wstawiające
def insert_postgres(conn, table, columns, data):
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(columns))
    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    cur.executemany(query, data)
    conn.commit()
    cur.close()

def insert_mariadb(conn, table, columns, data):
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(columns))
    query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
    cur.executemany(query, data)
    conn.commit()
    cur.close()

def insert_mongo(db, collection_name, columns, data):
    docs = [dict(zip(columns, row)) for row in data]
    db[collection_name].insert_many(docs, ordered=False)

def insert_neo4j(driver, node_label, columns, data):
    query = f"""
    UNWIND $batch AS row
    CREATE (n:{node_label})
    SET n += row
    """
    with driver.session() as session:
        batch = [dict(zip(columns, row)) for row in data]
        session.run(query, batch=batch)

def setup_neo4j_constraints(driver):
    print("Tworzenie constraintów (indeksów) w Neo4j...")
    labels = ["User", "Category", "Product", "Order", "OrderItem", "Review", "Cart", "CartItem", "Payment", "Shipment"]
    with driver.session() as session:
        for label in labels:
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")

def build_neo4j_relations(driver):
    print("Budowanie relacji w Neo4j (wymaga Neo4j 4.4+ dla transakcji wsadowych)...")
    # Zastosowano CALL {} IN TRANSACTIONS, aby zapobiec wyczerpaniu pamięci RAM
    queries = [
        "MATCH (p:Product) CALL { WITH p MATCH (c:Category {id: p.category_id}) CREATE (p)-[:BELONGS_TO]->(c) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (o:Order) CALL { WITH o MATCH (u:User {id: o.user_id}) CREATE (u)-[:PLACED]->(o) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (oi:OrderItem) CALL { WITH oi MATCH (o:Order {id: oi.order_id}) CREATE (o)-[:HAS_ITEM]->(oi) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (oi:OrderItem) CALL { WITH oi MATCH (p:Product {id: oi.product_id}) CREATE (oi)-[:FOR_PRODUCT]->(p) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (r:Review) CALL { WITH r MATCH (p:Product {id: r.product_id}) CREATE (r)-[:REVIEWS]->(p) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (r:Review) CALL { WITH r MATCH (u:User {id: r.user_id}) CREATE (u)-[:WROTE_REVIEW]->(r) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (c:Cart) CALL { WITH c MATCH (u:User {id: c.user_id}) CREATE (u)-[:HAS_CART]->(c) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (ci:CartItem) CALL { WITH ci MATCH (c:Cart {id: ci.cart_id}) CREATE (c)-[:CONTAINS]->(ci) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (ci:CartItem) CALL { WITH ci MATCH (p:Product {id: ci.product_id}) CREATE (ci)-[:FOR_PRODUCT]->(p) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (pay:Payment) CALL { WITH pay MATCH (o:Order {id: pay.order_id}) CREATE (pay)-[:PAYS_FOR]->(o) } IN TRANSACTIONS OF 10000 ROWS",
        "MATCH (s:Shipment) CALL { WITH s MATCH (o:Order {id: s.order_id}) CREATE (s)-[:SHIPS]->(o) } IN TRANSACTIONS OF 10000 ROWS"
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)

def seed_table(name, total_rows, generator_func, columns, dependencies, pg_conn, maria_conn, mongo_db, neo4j_driver, neo4j_label):
    print(f"--- Seeding {name} ({total_rows} records) ---")
    start = 1
    with tqdm(total=total_rows) as pbar:
        while start <= total_rows:
            end = min(start + CHUNK_SIZE, total_rows + 1)
            batch_data = generator_func(start, end, *dependencies)
            
            insert_postgres(pg_conn, name, columns, batch_data)
            insert_mariadb(maria_conn, name, columns, batch_data)
            insert_mongo(mongo_db, name, columns, batch_data)
            if neo4j_label:
                insert_neo4j(neo4j_driver, neo4j_label, columns, batch_data)
                
            pbar.update(end - start)
            start = end

def get_args():
    parser = argparse.ArgumentParser(description="Seed bazy danych e-commerce")
    parser.add_argument("--size", choices=['small', 'medium', 'large'], required=True, help="Wybierz profil wielkości danych")
    return parser.parse_args()

def main():
    args = get_args()
    print(f"Rozpoczynanie generowania danych. Profil: {args.size.upper()}")
    
    cfg = SIZES[args.size]
    pg_conn, maria_conn, mongo_db, neo4j_driver = get_db_connections()

    try:
        # Wymuszenie założenia indeksów w Neo4j przed wrzuceniem danych
        setup_neo4j_constraints(neo4j_driver)

        # 1. Users
        seed_table("users", cfg["users"], lambda s, e: gen_users(s, e), 
                   ["id", "username", "email", "created_at"], [], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "User")
                   
        # 2. Categories
        seed_table("categories", cfg["categories"], lambda s, e: gen_categories(s, e), 
                   ["id", "name", "description"], [], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Category")
                   
        # 3. Products
        seed_table("products", cfg["products"], lambda s, e, cat: gen_products(s, e, cat), 
                   ["id", "category_id", "name", "price", "stock"], [cfg["categories"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Product")
                   
        # 4. Orders
        seed_table("orders", cfg["orders"], lambda s, e, u: gen_orders(s, e, u), 
                   ["id", "user_id", "status", "total_amount", "created_at"], [cfg["users"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Order")
                   
        # 5. OrderItems
        seed_table("order_items", cfg["order_items"], lambda s, e, o, p: gen_order_items(s, e, o, p), 
                   ["id", "order_id", "product_id", "quantity", "unit_price"], [cfg["orders"], cfg["products"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "OrderItem")
                   
        # 6. Reviews
        seed_table("reviews", cfg["reviews"], lambda s, e, p, u: gen_reviews(s, e, p, u), 
                   ["id", "product_id", "user_id", "rating", "comment", "created_at"], [cfg["products"], cfg["users"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Review")
                   
        # 7. Carts
        seed_table("carts", cfg["carts"], lambda s, e, u: gen_carts(s, e, u), 
                   ["id", "user_id", "created_at"], [cfg["users"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Cart")
                   
        # 8. CartItems
        seed_table("cart_items", cfg["cart_items"], lambda s, e, c, p: gen_cart_items(s, e, c, p), 
                   ["id", "cart_id", "product_id", "quantity"], [cfg["carts"], cfg["products"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "CartItem")
                   
        # 9. Payments
        seed_table("payments", cfg["payments"], lambda s, e, o: gen_payments(s, e, o), 
                   ["id", "order_id", "amount", "method", "status", "payment_date"], [cfg["orders"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Payment")
                   
        # 10. Shipments
        seed_table("shipments", cfg["shipments"], lambda s, e, o: gen_shipments(s, e, o), 
                   ["id", "order_id", "tracking_number", "carrier", "status"], [cfg["orders"]], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Shipment")

        build_neo4j_relations(neo4j_driver)

        print("\nSukces! Dane wprowadzono do wszystkich baz.")

    except Exception as e:
        print(f"Wystąpił błąd: {e}")
    finally:
        pg_conn.close()
        maria_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()