--INSERT INTO customers (customer_id, customer_name) VALUES (1, 'Иван Иванов');
--INSERT INTO products (product_id, product_name, price) VALUES (1, 'Книга', 500);
--INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (1, 1, 1, 2);
--COMMIT;
--EXEC generate_changes_report(p_file_path => 'report_1.html', p_include_details => TRUE);

--UPDATE customers SET customer_name = 'Петр Петров' WHERE customer_id = 1;
--UPDATE products SET price = 600 WHERE product_id = 1;
--UPDATE orders SET quantity = 3 WHERE order_id = 1;
--COMMIT;
--EXEC generate_changes_report(p_file_path => 'report_2.html', p_include_details => TRUE);

--DELETE FROM orders WHERE order_id = 1;
--DELETE FROM products WHERE product_id = 1;
--DELETE FROM customers WHERE customer_id = 1;
--COMMIT;
--EXEC generate_changes_report(p_file_path => 'report_3.html', p_include_details => TRUE);

--SELECT SYSTIMESTAMP FROM DUAL; --24/04/25 23:52:18.291955000 GMT
--INSERT INTO customers (customer_id, customer_name) VALUES (2, 'Анна');
--UPDATE products SET price = 700 WHERE product_id = 1;
--COMMIT;
--EXEC generate_changes_report(p_file_path => 'report_4.html', p_include_details => TRUE);
--BEGIN
--  history_mgmt.rollback_to(TO_TIMESTAMP('2025-04-25 23:52:18.291', 'YYYY-MM-DD HH24:MI:SS.FF3'));
--END;
--/
--EXEC generate_changes_report(p_file_path => 'report_5.html', p_include_details => TRUE);

--INSERT INTO products (product_id, product_name, price) VALUES (2, 'Ручка', 50);
--COMMIT;
--SELECT * FROM customers_history;
--SELECT * FROM products_history;
--SELECT * FROM orders_history;
--SELECT * FROM customers;
--SELECT * FROM products;
--SELECT * FROM orders;
--BEGIN
--  history_mgmt.rollback_to(300000);
--END;
--/
--EXEC generate_changes_report(p_file_path => 'report_6.html', p_include_details => TRUE);
--SELECT * FROM customers_history;
--SELECT * FROM products_history;
--SELECT * FROM orders_history;
--SELECT * FROM customers;
--SELECT * FROM products;
--SELECT * FROM orders;
/* 
INSERT INTO customers (customer_id, customer_name) VALUES (1, 'Анна');
INSERT INTO customers (customer_id, customer_name) VALUES (2, 'Иван');
INSERT INTO customers (customer_id, customer_name) VALUES (3, 'Мария');

INSERT INTO products (product_id, product_name, price) VALUES (1, 'Ноутбук', 100000);
INSERT INTO products (product_id, product_name, price) VALUES (2, 'Телефон', 50000);
INSERT INTO products (product_id, product_name, price) VALUES (3, 'Наушники', 15000);

INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (1, 1, 1, 1);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (2, 2, 2, 2);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (3, 3, 3, 3);
COMMIT;
EXEC generate_changes_report(p_file_path => 'test1_inserts.html', p_include_details => TRUE);

UPDATE customers SET customer_name = 'Анна Петрова' WHERE customer_id = 1;
UPDATE customers SET customer_name = 'Иван Иванов' WHERE customer_id = 2;
UPDATE products SET price = 110000 WHERE product_id = 1;
UPDATE products SET price = 55000 WHERE product_id = 2;
UPDATE orders SET quantity = 5 WHERE order_id = 1;
UPDATE orders SET quantity = 10 WHERE order_id = 2;
COMMIT;
EXEC generate_changes_report(p_file_path => 'test2_updates.html', p_include_details => TRUE);

DELETE FROM customers WHERE customer_id = 3;
DELETE FROM products WHERE product_id = 3;
DELETE FROM orders WHERE order_id = 3;
COMMIT;
EXEC generate_changes_report(p_file_path => 'test3_deletes.html', p_include_details => TRUE);
 */
-- 2025-04-25 00:13:05
--EXEC generate_changes_report(p_since_timestamp => TO_TIMESTAMP('2025-04-25 00:13:05.450', 'YYYY-MM-DD HH24:MI:SS.FF3'), p_file_path => 'changes_report2.html', p_include_details => TRUE);
/* 
DECLARE
  v_start_time TIMESTAMP;
BEGIN
  SELECT SYSTIMESTAMP INTO v_start_time FROM DUAL;
  DBMS_OUTPUT.PUT_LINE('Тест 1: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS.FF3')); --Тест 1: 2025-04-25 00:36:31.962
END;
/

INSERT INTO customers (customer_id, customer_name) VALUES (1, 'Анна');
INSERT INTO customers (customer_id, customer_name) VALUES (2, 'Иван');
INSERT INTO products (product_id, product_name, price) VALUES (1, 'Ноутбук', 100000);
INSERT INTO products (product_id, product_name, price) VALUES (2, 'Телефон', 50000);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (1, 1, 1, 1);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (2, 2, 2, 2);
COMMIT;
UPDATE customers SET customer_name = 'Анна Петрова' WHERE customer_id = 1;
UPDATE products SET price = 110000 WHERE product_id = 1;
UPDATE orders SET quantity = 3 WHERE order_id = 1;
COMMIT;
DELETE FROM orders WHERE order_id = 2;
DELETE FROM products WHERE product_id = 2;
DELETE FROM customers WHERE customer_id = 2;
COMMIT;

EXEC generate_changes_report(p_file_path => 'test_report.html', p_include_details => TRUE);
 */
/* 
DECLARE
  v_start_time TIMESTAMP;
BEGIN
  SELECT SYSTIMESTAMP INTO v_start_time FROM DUAL;
  DBMS_OUTPUT.PUT_LINE('Тест 2: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS.FF3')); --Тест 2: 2025-04-25 00:39:06.222
END;
/

UPDATE customers SET customer_name = 'Карина Сергеевна' WHERE customer_id = 1;
UPDATE products SET price = 123123 WHERE product_id = 1;
UPDATE orders SET quantity = 56 WHERE order_id = 1;
COMMIT;

EXEC generate_changes_report(p_file_path => 'test2_updates.html', p_include_details => TRUE); */

--EXEC generate_changes_report(p_since_timestamp => TO_TIMESTAMP('2025-04-25 00:13:05.450', 'YYYY-MM-DD HH24:MI:SS.FF3'), p_file_path => 'testtesttest.html', p_include_details => TRUE);

/* INSERT INTO customers (customer_id, customer_name) VALUES (103, 'Test Customer');
INSERT INTO products (product_id, product_name, price) VALUES (102, 'Test Product', 99.99);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (102, 102, 102, 5);
COMMIT;

UPDATE customers SET customer_name = 'SSS' WHERE customer_id = 101;
UPDATE products SET product_name = 'SSS' WHERE product_id = 101;
UPDATE orders SET quantity = 100 WHERE order_id = 101;
COMMIT;

EXEC generate_changes_report(p_file_path => 'test_esche.html', p_include_details => TRUE); */

/* BEGIN
  history_mgmt.rollback_to(300000); -- 300000 мс = 5 минут
END;
/
EXEC generate_changes_report(p_file_path => 'report_9.html', p_include_details => TRUE); */
/* INSERT INTO customers (customer_id, customer_name) VALUES (98, 'Test Customer');
INSERT INTO products (product_id, product_name, price) VALUES (98, 'Test Product', 99.99);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (98, 98, 98, 5);
COMMIT;


EXEC generate_changes_report(p_file_path => 'design2.html', p_include_details => TRUE); 
 */
/* BEGIN
  history_mgmt.rollback_to(300000);
END;
/ */
EXEC generate_changes_report(p_since_timestamp => TO_TIMESTAMP('2025-04-25 00:12:00.450', 'YYYY-MM-DD HH24:MI:SS.FF3'), p_file_path => 'testt.html', p_include_details => TRUE);
