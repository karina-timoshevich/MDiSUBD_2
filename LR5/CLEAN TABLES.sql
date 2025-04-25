SELECT * FROM customers_history;
SELECT * FROM products_history;
SELECT * FROM orders_history;
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders;
ALTER TRIGGER customers_audit_trg DISABLE;
ALTER TRIGGER products_audit_trg DISABLE;
ALTER TRIGGER orders_audit_trg DISABLE;

DELETE FROM orders_history;
DELETE FROM products_history;
DELETE FROM customers_history;

DELETE FROM orders;
DELETE FROM products;
DELETE FROM customers;

ALTER SEQUENCE history_id_seq RESTART START WITH 1;
DELETE FROM report_tracking;
INSERT INTO report_tracking (last_report_time, report_count) 
VALUES (SYSTIMESTAMP, 0);

COMMIT;

ALTER TRIGGER customers_audit_trg ENABLE;
ALTER TRIGGER products_audit_trg ENABLE;
ALTER TRIGGER orders_audit_trg ENABLE;
SELECT * FROM customers_history;
SELECT * FROM products_history;
SELECT * FROM orders_history;
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders;