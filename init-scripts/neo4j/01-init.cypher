// Neo4j również nie wymaga tworzenia "pustych tabel", ale tworzymy Constrainty 
// na unikalne ID, które w świecie grafowym odpowiadają definicji encji.

CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE;
CREATE CONSTRAINT order_item_id IF NOT EXISTS FOR (oi:OrderItem) REQUIRE oi.id IS UNIQUE;
CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE;
CREATE CONSTRAINT cart_id IF NOT EXISTS FOR (cart:Cart) REQUIRE cart.id IS UNIQUE;
CREATE CONSTRAINT cart_item_id IF NOT EXISTS FOR (ci:CartItem) REQUIRE ci.id IS UNIQUE;
CREATE CONSTRAINT payment_id IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE;
CREATE CONSTRAINT shipment_id IF NOT EXISTS FOR (s:Shipment) REQUIRE s.id IS UNIQUE;
