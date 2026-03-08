import argparse
import random
from faker import Faker
import pg8000.dbapi
import mysql.connector
from pymongo import MongoClient
from neo4j import GraphDatabase
from tqdm import tqdm

fake = Faker()

# Konfiguracje połączeń
PG_CONFIG = {"host": "localhost", "port": 5432, "user": "ecom_user", "password": "ecom_password", "database": "ecommerce"}
MARIA_CONFIG = {"host": "127.0.0.1", "port": 3306, "user": "ecom_user", "password": "ecom_password", "database": "ecommerce"}
MONGO_URI = "mongodb://ecom_user:ecom_password@localhost:27017/"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "ecom_password"

CHUNK_SIZE = 10000

def get_db_connections():
    print("Łączenie z bazami danych...")
    pg_conn = pg8000.dbapi.connect(**PG_CONFIG)
    maria_conn = mysql.connector.connect(**MARIA_CONFIG)
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client["ecommerce"]
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    return pg_conn, maria_conn, mongo_db, neo4j_driver

# Generatory danych (zamiana datetime na str dla bezpieczeństwa typów)
def gen_users(start, end):
    return [(i, fake.user_name()[:50], fake.email()[:100], fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')) for i in range(start, end)]

def gen_categories(start, end):
    return [(i, fake.word()[:100], fake.sentence()) for i in range(start, end)]

def gen_products(start, end, max_cat):
    return [(i, random.randint(1, max_cat), fake.word()[:100], round(random.uniform(10, 1000), 2), random.randint(0, 1000)) for i in range(start, end)]

def gen_orders(start, end, max_user):
    return [(i, random.randint(1, max_user), random.choice(['PENDING', 'SHIPPED', 'COMPLETED', 'CANCELLED']), round(random.uniform(20, 2000), 2), fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')) for i in range(start, end)]

def gen_order_items(start, end, max_order, max_product):
    return [(i, random.randint(1, max_order), random.randint(1, max_product), random.randint(1, 5), round(random.uniform(10, 1000), 2)) for i in range(start, end)]

def gen_reviews(start, end, max_product, max_user):
    return [(i, random.randint(1, max_product), random.randint(1, max_user), random.randint(1, 5), fake.text()[:200], fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')) for i in range(start, end)]

def gen_carts(start, end, max_user):
    return [(i, random.randint(1, max_user), fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')) for i in range(start, end)]

def gen_cart_items(start, end, max_cart, max_product):
    return [(i, random.randint(1, max_cart), random.randint(1, max_product), random.randint(1, 10)) for i in range(start, end)]

def gen_payments(start, end, max_order):
    return [(i, random.randint(1, max_order), round(random.uniform(20, 2000), 2), random.choice(['CREDIT_CARD', 'PAYPAL', 'BLIK', 'TRANSFER']), random.choice(['SUCCESS', 'FAILED', 'PENDING']), fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S')) for i in range(start, end)]

def gen_shipments(start, end, max_order):
    return [(i, random.randint(1, max_order), fake.ean13(), random.choice(['DPD', 'INPOST', 'DHL', 'UPS']), random.choice(['IN_TRANSIT', 'DELIVERED', 'RETURNED'])) for i in range(start, end)]

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
        # konwersja na twarde dicty
        batch = [dict(zip(columns, row)) for row in data]
        session.run(query, batch=batch)

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

def build_neo4j_relations(driver):
    print("Budowanie relacji w Neo4j...")
    queries = [
        "MATCH (p:Product), (c:Category) WHERE p.category_id = c.id CREATE (p)-[:BELONGS_TO]->(c)",
        "MATCH (o:Order), (u:User) WHERE o.user_id = u.id CREATE (u)-[:PLACED]->(o)",
        "MATCH (oi:OrderItem), (o:Order) WHERE oi.order_id = o.id CREATE (o)-[:HAS_ITEM]->(oi)",
        "MATCH (oi:OrderItem), (p:Product) WHERE oi.product_id = p.id CREATE (oi)-[:FOR_PRODUCT]->(p)",
        "MATCH (r:Review), (p:Product) WHERE r.product_id = p.id CREATE (r)-[:REVIEWS]->(p)",
        "MATCH (r:Review), (u:User) WHERE r.user_id = u.id CREATE (u)-[:WROTE_REVIEW]->(r)",
        "MATCH (c:Cart), (u:User) WHERE c.user_id = u.id CREATE (u)-[:HAS_CART]->(c)",
        "MATCH (ci:CartItem), (c:Cart) WHERE ci.cart_id = c.id CREATE (c)-[:CONTAINS]->(ci)",
        "MATCH (ci:CartItem), (p:Product) WHERE ci.product_id = p.id CREATE (ci)-[:FOR_PRODUCT]->(p)",
        "MATCH (pay:Payment), (o:Order) WHERE pay.order_id = o.id CREATE (pay)-[:PAYS_FOR]->(o)",
        "MATCH (s:Shipment), (o:Order) WHERE s.order_id = o.id CREATE (s)-[:SHIPS]->(o)"
    ]
    with driver.session() as session:
        for q in tqdm(queries):
            session.run(q)

def get_args():
    parser = argparse.ArgumentParser(description="Seed bazy danych")
    parser.add_argument("N", type=int, help="Wielkosc probki (np 10000)")
    return parser.parse_args()

def main():
    args = get_args()
    N = args.N
    print(f"Rozpoczynanie generowania danych dla N={N}")
    
    pg_conn, maria_conn, mongo_db, neo4j_driver = get_db_connections()

    try:
        # Konfiguracja wielkości encji
        num_users = N
        num_categories = max(10, N // 100)
        num_products = N
        num_orders = N * 2
        num_order_items = N * 4
        num_reviews = N
        num_carts = N
        num_cart_items = N * 2
        num_payments = N * 2
        num_shipments = N * 2

        # 1. Users
        seed_table("users", num_users, lambda s, e: gen_users(s, e), 
                   ["id", "username", "email", "created_at"], [], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "User")
                   
        # 2. Categories
        seed_table("categories", num_categories, lambda s, e: gen_categories(s, e), 
                   ["id", "name", "description"], [], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Category")
                   
        # 3. Products
        seed_table("products", num_products, lambda s, e, cat: gen_products(s, e, cat), 
                   ["id", "category_id", "name", "price", "stock"], [num_categories], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Product")
                   
        # 4. Orders
        seed_table("orders", num_orders, lambda s, e, u: gen_orders(s, e, u), 
                   ["id", "user_id", "status", "total_amount", "created_at"], [num_users], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Order")
                   
        # 5. OrderItems
        seed_table("order_items", num_order_items, lambda s, e, o, p: gen_order_items(s, e, o, p), 
                   ["id", "order_id", "product_id", "quantity", "unit_price"], [num_orders, num_products], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "OrderItem")
                   
        # 6. Reviews
        seed_table("reviews", num_reviews, lambda s, e, p, u: gen_reviews(s, e, p, u), 
                   ["id", "product_id", "user_id", "rating", "comment", "created_at"], [num_products, num_users], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Review")
                   
        # 7. Carts
        seed_table("carts", num_carts, lambda s, e, u: gen_carts(s, e, u), 
                   ["id", "user_id", "created_at"], [num_users], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Cart")
                   
        # 8. CartItems
        seed_table("cart_items", num_cart_items, lambda s, e, c, p: gen_cart_items(s, e, c, p), 
                   ["id", "cart_id", "product_id", "quantity"], [num_carts, num_products], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "CartItem")
                   
        # 9. Payments
        seed_table("payments", num_payments, lambda s, e, o: gen_payments(s, e, o), 
                   ["id", "order_id", "amount", "method", "status", "payment_date"], [num_orders], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Payment")
                   
        # 10. Shipments
        seed_table("shipments", num_shipments, lambda s, e, o: gen_shipments(s, e, o), 
                   ["id", "order_id", "tracking_number", "carrier", "status"], [num_orders], 
                   pg_conn, maria_conn, mongo_db, neo4j_driver, "Shipment")

        # Tworzenie krawędzi (Edges) w Neo4j na podstawie ID (Z powodu struktury relacyjnej)
        # Przy milionie danych to może potrwać, można to zoptymalizować uźywając APOC lub CALL {} IN TRANSACTIONS ale dla prostoty użyjemy bezpośrednich złączeń (jeśli mają index - tak, zrobiliśmy Constraint u:User(id) )
        build_neo4j_relations(neo4j_driver)

        print("\nSukces! Dane wprowadzono do wszystkich 4 baz.")

    except Exception as e:
        print(f"Wystąpił błąd: {e}")
    finally:
        pg_conn.close()
        maria_conn.close()
        neo4j_driver.close()

if __name__ == "__main__":
    main()
