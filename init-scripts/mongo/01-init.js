// MongoDB jest schemaless, ale możemy jawnie utworzyć kolekcje (odpowiedniki tabel)
// W środowisku NoSQL nie pre-definiujemy typów kolumn na tym etapie.
db = db.getSiblingDB('ecommerce');

db.createCollection("users");
db.createCollection("categories");
db.createCollection("products");
db.createCollection("orders");
db.createCollection("order_items");
db.createCollection("reviews");
db.createCollection("carts");
db.createCollection("cart_items");
db.createCollection("payments");
db.createCollection("shipments");

print("MongoDB: 10 collections created successfully.");
