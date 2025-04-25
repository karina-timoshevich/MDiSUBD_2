BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers_history CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE products_history CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders_history CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE history_id_seq'; EXCEPTION WHEN OTHERS THEN NULL; END;
END;
/

CREATE SEQUENCE history_id_seq
  START WITH 1
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;

CREATE TABLE customers (
  customer_id     NUMBER(6)      PRIMARY KEY,
  customer_name   VARCHAR2(100)  NOT NULL,
  registered_at   DATE           DEFAULT SYSDATE
);

CREATE TABLE products (
  product_id      NUMBER(6)      PRIMARY KEY,
  product_name    VARCHAR2(100)  NOT NULL,
  price           NUMBER(10, 2)  NOT NULL
);

CREATE TABLE orders (
  order_id        NUMBER(10)     PRIMARY KEY,
  customer_id     NUMBER(6),
  product_id      NUMBER(6),
  order_date      DATE           DEFAULT SYSDATE,
  quantity        NUMBER(5),

  CONSTRAINT fk_order_customer FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id),
  
  CONSTRAINT fk_order_product FOREIGN KEY (product_id)
    REFERENCES products(product_id)
);

CREATE TABLE customers_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,  
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  customer_id     NUMBER(6)      NOT NULL,
  customer_name   VARCHAR2(100),
  old_name        VARCHAR2(100),  
  registered_at   DATE,
  old_reg_date    DATE,
  rolled_back     NUMBER(1)      DEFAULT 0  
);

CREATE TABLE products_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  product_id      NUMBER(6)      NOT NULL,
  product_name    VARCHAR2(100),
  old_name        VARCHAR2(100),  
  price           NUMBER(10, 2),
  old_price       NUMBER(10, 2),
  rolled_back     NUMBER(1)      DEFAULT 0  
);

CREATE TABLE orders_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  order_id        NUMBER(10)     NOT NULL,
  customer_id     NUMBER(6),
  old_customer_id NUMBER(6),      
  product_id      NUMBER(6),
  old_product_id  NUMBER(6),      
  order_date      DATE,
  old_order_date  DATE,           
  quantity        NUMBER(5),
  old_quantity    NUMBER(5),
  rolled_back     NUMBER(1)      DEFAULT 0  
);

CREATE OR REPLACE TRIGGER customers_audit_trg
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW
DECLARE
  v_operation VARCHAR2(10);
BEGIN
  IF INSERTING THEN
    v_operation := 'INSERT';
  ELSIF UPDATING THEN
    v_operation := 'UPDATE';
  ELSIF DELETING THEN
    v_operation := 'DELETE';
  END IF;
  
  INSERT INTO customers_history (
    history_id, operation_type, customer_id, 
    customer_name, old_name, 
    registered_at, old_reg_date
  ) VALUES (
    history_id_seq.NEXTVAL, v_operation,
    CASE 
      WHEN v_operation = 'DELETE' THEN :OLD.customer_id
      ELSE :NEW.customer_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.customer_name
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.customer_name
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.registered_at
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.registered_at
    END
  );
END;
/

CREATE OR REPLACE TRIGGER products_audit_trg
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW
DECLARE
  v_operation VARCHAR2(10);
BEGIN
  IF INSERTING THEN
    v_operation := 'INSERT';
  ELSIF UPDATING THEN
    v_operation := 'UPDATE';
  ELSIF DELETING THEN
    v_operation := 'DELETE';
  END IF;
  
  INSERT INTO products_history (
    history_id, operation_type, product_id, 
    product_name, old_name, 
    price, old_price
  ) VALUES (
    history_id_seq.NEXTVAL, v_operation,
    CASE 
      WHEN v_operation = 'DELETE' THEN :OLD.product_id
      ELSE :NEW.product_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.product_name
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.product_name
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.price
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.price
    END
  );
END;
/

CREATE OR REPLACE TRIGGER orders_audit_trg
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
DECLARE
  v_operation VARCHAR2(10);
BEGIN
  IF INSERTING THEN
    v_operation := 'INSERT';
  ELSIF UPDATING THEN
    v_operation := 'UPDATE';
  ELSIF DELETING THEN
    v_operation := 'DELETE';
  END IF;
  
  INSERT INTO orders_history (
    history_id, operation_type, order_id, 
    customer_id, old_customer_id,
    product_id, old_product_id,
    order_date, old_order_date,
    quantity, old_quantity
  ) VALUES (
    history_id_seq.NEXTVAL, v_operation,
    CASE 
      WHEN v_operation = 'DELETE' THEN :OLD.order_id
      ELSE :NEW.order_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.customer_id
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.customer_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.product_id
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.product_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.order_date
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.order_date
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.quantity
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.quantity
    END
  );
END;
/

ALTER TABLE orders DROP CONSTRAINT fk_order_customer;
ALTER TABLE orders ADD CONSTRAINT fk_order_customer
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
ON DELETE CASCADE;

ALTER TABLE orders DROP CONSTRAINT fk_order_product;
ALTER TABLE orders ADD CONSTRAINT fk_order_product
FOREIGN KEY (product_id) REFERENCES products(product_id)
ON DELETE CASCADE;

CREATE OR REPLACE PACKAGE history_mgmt AS
  
  PROCEDURE rollback_to(p_timestamp IN TIMESTAMP);
  PROCEDURE rollback_to(p_milliseconds IN NUMBER);
  PROCEDURE show_changes_after(p_timestamp IN TIMESTAMP);
  
END history_mgmt;
/

CREATE OR REPLACE PACKAGE BODY history_mgmt AS

  PROCEDURE do_rollback(p_timestamp IN TIMESTAMP) IS
    v_exists NUMBER;
  BEGIN
    SAVEPOINT rollback_start; 

    EXECUTE IMMEDIATE 'ALTER TRIGGER customers_audit_trg DISABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER products_audit_trg DISABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER orders_audit_trg DISABLE';
    
    BEGIN
      FOR r_cust IN (
        SELECT history_id, customer_id, customer_name, registered_at, old_name, old_reg_date, operation_type, change_time
        FROM customers_history
        WHERE change_time > p_timestamp
        ORDER BY change_time DESC
      ) LOOP
        CASE r_cust.operation_type
          WHEN 'INSERT' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing INSERT: Deleting customer ' || r_cust.customer_id);
                DELETE FROM customers WHERE customer_id = r_cust.customer_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing customer insert: ' || SQLERRM);
            END;
            
          WHEN 'UPDATE' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing UPDATE: Restoring customer ' || r_cust.customer_id || ' to previous state');
                UPDATE customers
                SET customer_name = r_cust.old_name,
                    registered_at = r_cust.old_reg_date
                WHERE customer_id = r_cust.customer_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing customer update: ' || SQLERRM);
            END;
            
          WHEN 'DELETE' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
              IF v_exists = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing DELETE: Restoring customer ' || r_cust.customer_id);
                INSERT INTO customers (customer_id, customer_name, registered_at)
                VALUES (r_cust.customer_id, r_cust.old_name, r_cust.old_reg_date);
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing customer delete: ' || SQLERRM);
            END;
        END CASE;
        UPDATE customers_history SET rolled_back = 1 WHERE history_id = r_cust.history_id;
      END LOOP;

      FOR r_prod IN (
        SELECT history_id, product_id, product_name, price, old_name, old_price, operation_type, change_time
        FROM products_history
        WHERE change_time > p_timestamp
        ORDER BY change_time DESC
      ) LOOP
        CASE r_prod.operation_type
          WHEN 'INSERT' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing INSERT: Deleting product ' || r_prod.product_id);
                DELETE FROM products WHERE product_id = r_prod.product_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing product insert: ' || SQLERRM);
            END;
            
          WHEN 'UPDATE' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing UPDATE: Restoring product ' || r_prod.product_id || ' to previous state');
                UPDATE products
                SET product_name = r_prod.old_name,
                    price = r_prod.old_price
                WHERE product_id = r_prod.product_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing product update: ' || SQLERRM);
            END;
            
          WHEN 'DELETE' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
              IF v_exists = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing DELETE: Restoring product ' || r_prod.product_id);
                INSERT INTO products (product_id, product_name, price)
                VALUES (r_prod.product_id, r_prod.old_name, r_prod.old_price);
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing product delete: ' || SQLERRM);
            END;
        END CASE;
        UPDATE products_history SET rolled_back = 1 WHERE history_id = r_prod.history_id;
      END LOOP;

      FOR r_ord IN (
        SELECT history_id, order_id, customer_id, product_id, order_date, quantity, operation_type, change_time, old_quantity, old_order_date, old_product_id, old_customer_id
        FROM orders_history
        WHERE change_time > p_timestamp
        ORDER BY change_time DESC
      ) LOOP
        CASE r_ord.operation_type
          WHEN 'INSERT' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing INSERT: Deleting order ' || r_ord.order_id);
                DELETE FROM orders WHERE order_id = r_ord.order_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing order insert: ' || SQLERRM);
            END;
            
          WHEN 'UPDATE' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing UPDATE: Restoring order ' || r_ord.order_id || ' to previous state');
                UPDATE orders
                SET customer_id = r_ord.old_customer_id,
                    product_id = r_ord.old_product_id,
                    order_date = r_ord.old_order_date,
                    quantity = r_ord.old_quantity
                WHERE order_id = r_ord.order_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing order update: ' || SQLERRM);
            END;
            
          WHEN 'DELETE' THEN
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
              IF v_exists = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing DELETE: Restoring order ' || r_ord.order_id);
                INSERT INTO orders (order_id, customer_id, product_id, order_date, quantity)
                VALUES (r_ord.order_id, r_ord.old_customer_id, r_ord.old_product_id, r_ord.old_order_date, r_ord.old_quantity);
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing order delete: ' || SQLERRM);
            END;
        END CASE;
        UPDATE orders_history SET rolled_back = 1 WHERE history_id = r_ord.history_id;
      END LOOP;

      EXECUTE IMMEDIATE 'ALTER TRIGGER customers_audit_trg ENABLE';
      EXECUTE IMMEDIATE 'ALTER TRIGGER products_audit_trg ENABLE';
      EXECUTE IMMEDIATE 'ALTER TRIGGER orders_audit_trg ENABLE';
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Rollback to ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3') || ' completed successfully.');
      
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK TO rollback_start;
        EXECUTE IMMEDIATE 'ALTER TRIGGER customers_audit_trg ENABLE';
        EXECUTE IMMEDIATE 'ALTER TRIGGER products_audit_trg ENABLE';
        EXECUTE IMMEDIATE 'ALTER TRIGGER orders_audit_trg ENABLE';
        DBMS_OUTPUT.PUT_LINE('Error during rollback: ' || SQLERRM);
        RAISE;
    END;
  END do_rollback;

  PROCEDURE rollback_to(p_timestamp IN TIMESTAMP) IS
  BEGIN
    do_rollback(p_timestamp);
  END rollback_to;
  
  PROCEDURE rollback_to(p_milliseconds IN NUMBER) IS
    v_timestamp TIMESTAMP;
  BEGIN
    SELECT SYSTIMESTAMP - NUMTODSINTERVAL(p_milliseconds/1000, 'SECOND') 
    INTO v_timestamp 
    FROM DUAL;
    
    DBMS_OUTPUT.PUT_LINE('Rolling back to ' || TO_CHAR(v_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
                         ' (' || p_milliseconds || ' milliseconds ago)');
    do_rollback(v_timestamp);
  END rollback_to;
  
  PROCEDURE show_changes_after(p_timestamp IN TIMESTAMP) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('Changes to Customers after ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'));
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR c IN (
      SELECT operation_type, change_time, customer_id, customer_name, old_name, registered_at, old_reg_date
      FROM customers_history
      WHERE change_time > p_timestamp
      ORDER BY change_time
    ) LOOP
      DBMS_OUTPUT.PUT_LINE(
        c.operation_type || ' at ' || TO_CHAR(c.change_time, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
        ' | ID: ' || c.customer_id || 
        ' | Name: ' || COALESCE(c.customer_name, 'NULL') || 
        ' | Old Name: ' || COALESCE(c.old_name, 'NULL') ||
        ' | Reg Date: ' || COALESCE(TO_CHAR(c.registered_at, 'YYYY-MM-DD'), 'NULL') ||
        ' | Old Reg Date: ' || COALESCE(TO_CHAR(c.old_reg_date, 'YYYY-MM-DD'), 'NULL')
      );
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Changes to Products after ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'));
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR p IN (
      SELECT operation_type, change_time, product_id, product_name, old_name, price, old_price
      FROM products_history
      WHERE change_time > p_timestamp
      ORDER BY change_time
    ) LOOP
      DBMS_OUTPUT.PUT_LINE(
        p.operation_type || ' at ' || TO_CHAR(p.change_time, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
        ' | ID: ' || p.product_id || 
        ' | Name: ' || COALESCE(p.product_name, 'NULL') || 
        ' | Old Name: ' || COALESCE(p.old_name, 'NULL') ||
        ' | Price: ' || COALESCE(TO_CHAR(p.price), 'NULL') ||
        ' | Old Price: ' || COALESCE(TO_CHAR(p.old_price), 'NULL')
      );
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Changes to Orders after ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'));
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR o IN (
      SELECT operation_type, change_time, order_id, 
             customer_id, old_customer_id, 
             product_id, old_product_id, 
             order_date, old_order_date,
             quantity, old_quantity
      FROM orders_history
      WHERE change_time > p_timestamp
      ORDER BY change_time
    ) LOOP
      DBMS_OUTPUT.PUT_LINE(
        o.operation_type || ' at ' || TO_CHAR(o.change_time, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
        ' | Order ID: ' || o.order_id || 
        ' | Customer: ' || COALESCE(TO_CHAR(o.customer_id), 'NULL') || 
        ' | Old Customer: ' || COALESCE(TO_CHAR(o.old_customer_id), 'NULL') ||
        ' | Product: ' || COALESCE(TO_CHAR(o.product_id), 'NULL') ||
        ' | Old Product: ' || COALESCE(TO_CHAR(o.old_product_id), 'NULL') ||
        ' | Qty: ' || COALESCE(TO_CHAR(o.quantity), 'NULL') ||
        ' | Old Qty: ' || COALESCE(TO_CHAR(o.old_quantity), 'NULL')
      );
    END LOOP;
  END show_changes_after;
  
END history_mgmt;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE report_tracking';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE report_tracking (
  last_report_time TIMESTAMP,
  report_count NUMBER DEFAULT 0
);

INSERT INTO report_tracking (last_report_time, report_count) 
VALUES (SYSTIMESTAMP, 0);
COMMIT;
/

BEGIN
  EXECUTE IMMEDIATE 'DROP DIRECTORY MY_DIR';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE OR REPLACE DIRECTORY MY_DIR AS '/opt/oracle/reports';

CREATE OR REPLACE PROCEDURE generate_changes_report(
  p_since_timestamp IN TIMESTAMP DEFAULT NULL,
  p_file_path IN VARCHAR2,
  p_include_details IN BOOLEAN DEFAULT FALSE
) AS
  v_file UTL_FILE.FILE_TYPE;
  v_start_time TIMESTAMP;
  v_end_time TIMESTAMP := SYSTIMESTAMP;
  v_report_num NUMBER;
  v_filename VARCHAR2(255);
  v_cust_inserts NUMBER := 0;
  v_cust_updates NUMBER := 0;
  v_cust_deletes NUMBER := 0;
  v_prod_inserts NUMBER := 0;
  v_prod_updates NUMBER := 0;
  v_prod_deletes NUMBER := 0;
  v_order_inserts NUMBER := 0;
  v_order_updates NUMBER := 0;
  v_order_deletes NUMBER := 0;
  
  PROCEDURE write_html(p_content IN VARCHAR2) IS
  BEGIN
    UTL_FILE.PUT_LINE(v_file, p_content);
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error writing to file: ' || SQLERRM);
      RAISE;
  END write_html;
  
BEGIN
  IF p_since_timestamp IS NULL THEN
    SELECT last_report_time, report_count+1
    INTO v_start_time, v_report_num
    FROM report_tracking;
  ELSE
    v_start_time := p_since_timestamp;
    SELECT report_count+1
    INTO v_report_num
    FROM report_tracking;
  END IF;
  
  SELECT 
    COUNT(CASE WHEN operation_type = 'INSERT' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'UPDATE' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'DELETE' AND rolled_back = 0 THEN 1 END)
  INTO v_cust_inserts, v_cust_updates, v_cust_deletes
  FROM customers_history
  WHERE change_time BETWEEN v_start_time AND v_end_time;
  
  SELECT 
    COUNT(CASE WHEN operation_type = 'INSERT' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'UPDATE' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'DELETE' AND rolled_back = 0 THEN 1 END)
  INTO v_prod_inserts, v_prod_updates, v_prod_deletes
  FROM products_history
  WHERE change_time BETWEEN v_start_time AND v_end_time;
  
  SELECT 
    COUNT(CASE WHEN operation_type = 'INSERT' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'UPDATE' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'DELETE' AND rolled_back = 0 THEN 1 END)
  INTO v_order_inserts, v_order_updates, v_order_deletes
  FROM orders_history
  WHERE change_time BETWEEN v_start_time AND v_end_time;
  
  v_filename := p_file_path;
  
  BEGIN
    BEGIN
      DBMS_OUTPUT.PUT_LINE('Attempting to open file: ' || v_filename || ' in directory MY_DIR');
      v_file := UTL_FILE.FOPEN('MY_DIR', v_filename, 'W', 32767);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error opening file: ' || SQLERRM || ' - ' || SQLCODE);
        RAISE;
    END;
     write_html('<!DOCTYPE html>');
    write_html('<html lang="ru">');
    write_html('<head>');
    write_html('  <meta charset="UTF-8">');
    write_html('  <title>Отчет об изменениях БД #' || v_report_num || '</title>');
    write_html('  <style>');
    write_html('    * { box-sizing: border-box; margin: 0; padding: 0; }');
    write_html('    body { font-family: "Roboto Condensed", sans-serif; background: #f8f9fa; }');
    write_html('    .container { max-width: 1200px; margin: 2rem auto; padding: 0 20px; }');
    write_html('    .header { background: linear-gradient(135deg, #2c3e50, #3498db); color: white; padding: 2rem; border-radius: 10px 10px 0 0; }');
    write_html('    .report-title { font-size: 2.2rem; margin-bottom: 0.5rem; }');
    write_html('    .report-info { display: flex; gap: 2rem; margin-top: 1.5rem; }');
    write_html('    .badge { display: inline-block; padding: 4px 12px; border-radius: 20px; font-weight: 500; }');
    write_html('    .insert-badge { background: #27ae60; color: white; }');
    write_html('    .update-badge { background: #2980b9; color: white; }');
    write_html('    .delete-badge { background: #e74c3c; color: white; }');
    write_html('    .card { background: white; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-top: 2rem; }');
    write_html('    table { width: 100%; border-collapse: collapse; }');
    write_html('    th { background: #f8f9fa; color: #2c3e50; padding: 1rem; text-align: left; border-bottom: 2px solid #eee; }');
    write_html('    td { padding: 1rem; border-bottom: 1px solid #eee; }');
    write_html('    tr:hover { background-color: #f8f9fa; }');
    write_html('    .total-row { font-weight: 600; background-color: #f8f9fa; }');
    write_html('    .timestamp { color: #7f8c8d; font-size: 0.9em; }');
    write_html('    .section-title { font-size: 1.4rem; color: #2c3e50; margin: 2rem 0 1rem; }');
    write_html('    .footer { text-align: center; color: #95a5a6; padding: 1.5rem; font-size: 0.9rem; }');
    write_html('  </style>');
    write_html('</head>');
    write_html('<body>');
    
    write_html('  <div class="header">');
    write_html('    <div class="container">');
    write_html('      <h1 class="report-title">Отчет об изменениях базы данных</h1>');
    write_html('      <div class="report-info">');
    write_html('        <div>');
    write_html('          <div class="timestamp">Отчёт сгенерирован: ' || TO_CHAR(v_end_time, 'YYYY-MM-DD HH24:MI:SS') || '</div>');
    write_html('          <div class="timestamp">Период: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS') || ' — ' || TO_CHAR(v_end_time, 'YYYY-MM-DD HH24:MI:SS') || '</div>');
    write_html('        </div>');
    write_html('      </div>');
    write_html('    </div>');
    write_html('  </div>');
    
    write_html('  <div class="container">');
    write_html('    <div class="card">');
    write_html('      <h2 class="section-title">Сводка изменений</h2>');
    write_html('      <table>');
    write_html('        <thead>');
    write_html('          <tr>');
    write_html('            <th>Таблица</th>');
    write_html('            <th>Добавлено</th>');
    write_html('            <th>Изменено</th>');
    write_html('            <th>Удалено</th>');
    write_html('            <th>Всего</th>');
    write_html('          </tr>');
    write_html('        </thead>');
    write_html('        <tbody>');

    write_html('          <tr>');
    write_html('            <td>Клиенты</td>');
    write_html('            <td><span class="badge insert-badge">+' || v_cust_inserts || '</span></td>');
    write_html('            <td><span class="badge update-badge">~' || v_cust_updates || '</span></td>');
    write_html('            <td><span class="badge delete-badge">-' || v_cust_deletes || '</span></td>');
    write_html('            <td>' || (v_cust_inserts + v_cust_updates + v_cust_deletes) || '</td>');
    write_html('          </tr>');

    write_html('          <tr>');
    write_html('            <td>Товары</td>');
    write_html('            <td><span class="badge insert-badge">+' || v_prod_inserts || '</span></td>');
    write_html('            <td><span class="badge update-badge">~' || v_prod_updates || '</span></td>');
    write_html('            <td><span class="badge delete-badge">-' || v_prod_deletes || '</span></td>');
    write_html('            <td>' || (v_prod_inserts + v_prod_updates + v_prod_deletes) || '</td>');
    write_html('          </tr>');
    
    write_html('          <tr>');
    write_html('            <td>Заказы</td>');
    write_html('            <td><span class="badge insert-badge">+' || v_order_inserts || '</span></td>');
    write_html('            <td><span class="badge update-badge">~' || v_order_updates || '</span></td>');
    write_html('            <td><span class="badge delete-badge">-' || v_order_deletes || '</span></td>');
    write_html('            <td>' || (v_order_inserts + v_order_updates + v_order_deletes) || '</td>');
    write_html('          </tr>');

    write_html('          <tr class="total-row">');
    write_html('            <td>Итого</td>');
    write_html('            <td><span class="badge insert-badge">+' || (v_cust_inserts + v_prod_inserts + v_order_inserts) || '</span></td>');
    write_html('            <td><span class="badge update-badge">~' || (v_cust_updates + v_prod_updates + v_order_updates) || '</span></td>');
    write_html('            <td><span class="badge delete-badge">-' || (v_cust_deletes + v_prod_deletes + v_order_deletes) || '</span></td>');
    write_html('            <td>' || (v_cust_inserts + v_cust_updates + v_cust_deletes + v_prod_inserts + v_prod_updates + v_prod_deletes + v_order_inserts + v_order_updates + v_order_deletes) || '</td>');
    write_html('          </tr>');
    
    write_html('        </tbody>');
    write_html('      </table>');
    write_html('    </div>');
    
    IF p_include_details THEN
      IF (v_cust_inserts + v_cust_updates + v_cust_deletes) > 0 THEN
         write_html('    <div class="card">');
        write_html('      <h2 class="section-title">Изменения клиентов</h2>');
        write_html('      <table>');
        write_html('        <thead>');
        write_html('          <tr>');
        write_html('            <th>Время</th>');
        write_html('            <th>Операция</th>');
        write_html('            <th>ID</th>');
        write_html('            <th>Имя</th>');
        write_html('            <th>Предыдущее имя</th>');
        write_html('          </tr>');
        write_html('        </thead>');
        write_html('        <tbody>');
        
        FOR c IN (
          SELECT operation_type, change_time, customer_id, customer_name, old_name
          FROM customers_history
          WHERE change_time BETWEEN v_start_time AND v_end_time AND rolled_back = 0
          ORDER BY change_time
        ) LOOP
           write_html('          <tr>');
          write_html('            <td class="timestamp">' || TO_CHAR(c.change_time, 'YYYY-MM-DD HH24:MI:SS') || '</td>');
          write_html('            <td><span class="badge ' || LOWER(c.operation_type) || '-badge">' || c.operation_type || '</span></td>');
          write_html('            <td>' || c.customer_id || '</td>');
          write_html('            <td>' || COALESCE(c.customer_name, '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(c.old_name, '<span class="null-value">N/A</span>') || '</td>');
          write_html('          </tr>');
        END LOOP;
         write_html('        </tbody>');
        write_html('      </table>');
        write_html('    </div>');
      END IF;
      
      IF (v_prod_inserts + v_prod_updates + v_prod_deletes) > 0 THEN
          write_html('    <div class="card">');
        write_html('      <h2 class="section-title">Изменения товаров</h2>');
        write_html('      <table>');
        write_html('        <thead>');
        write_html('          <tr>');
        write_html('            <th>Время</th>');
        write_html('            <th>Операция</th>');
        write_html('            <th>ID</th>');
        write_html('            <th>Наименование</th>');
        write_html('            <th>Пред. наименование</th>');
        write_html('            <th>Цена</th>');
        write_html('            <th>Пред. цена</th>');
        write_html('          </tr>');
        write_html('        </thead>');
        write_html('        <tbody>');

        FOR p IN (
          SELECT operation_type, change_time, product_id, product_name, old_name, price, old_price
          FROM products_history
          WHERE change_time BETWEEN v_start_time AND v_end_time AND rolled_back = 0
          ORDER BY change_time
        ) LOOP
          write_html('          <tr>');
          write_html('            <td class="timestamp">' || TO_CHAR(p.change_time, 'YYYY-MM-DD HH24:MI:SS') || '</td>');
          write_html('            <td><span class="badge ' || LOWER(p.operation_type) || '-badge">' || p.operation_type || '</span></td>');
          write_html('            <td>' || p.product_id || '</td>');
          write_html('            <td>' || COALESCE(p.product_name, '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(p.old_name, '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(p.price, 'FM999G999G990D00'), '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(p.old_price, 'FM999G999G990D00'), '<span class="null-value">N/A</span>') || '</td>');
          write_html('          </tr>');
        END LOOP;
        
        write_html('        </tbody>');
        write_html('      </table>');
        write_html('    </div>');
      END IF;
      
      IF (v_order_inserts + v_order_updates + v_order_deletes) > 0 THEN
         write_html('    <div class="card">');
        write_html('      <h2 class="section-title">Изменения заказов</h2>');
        write_html('      <table>');
        write_html('        <thead>');
        write_html('          <tr>');
        write_html('            <th>Время</th>');
        write_html('            <th>Операция</th>');
        write_html('            <th>ID заказа</th>');
        write_html('            <th>Клиент</th>');
        write_html('            <th>Пред. клиент</th>');
        write_html('            <th>Товар</th>');
        write_html('            <th>Пред. товар</th>');
        write_html('            <th>Кол-во</th>');
        write_html('            <th>Пред. кол-во</th>');
        write_html('          </tr>');
        write_html('        </thead>');
        write_html('        <tbody>');
        FOR o IN (
          SELECT operation_type, change_time, order_id, 
                 customer_id, old_customer_id, 
                 product_id, old_product_id,
                 quantity, old_quantity
          FROM orders_history
          WHERE change_time BETWEEN v_start_time AND v_end_time AND rolled_back = 0
          ORDER BY change_time
        ) LOOP
           write_html('          <tr>');
          write_html('            <td class="timestamp">' || TO_CHAR(o.change_time, 'YYYY-MM-DD HH24:MI:SS') || '</td>');
          write_html('            <td><span class="badge ' || LOWER(o.operation_type) || '-badge">' || o.operation_type || '</span></td>');
          write_html('            <td>' || o.order_id || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(o.customer_id), '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(o.old_customer_id), '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(o.product_id), '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(o.old_product_id), '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(o.quantity), '<span class="null-value">N/A</span>') || '</td>');
          write_html('            <td>' || COALESCE(TO_CHAR(o.old_quantity), '<span class="null-value">N/A</span>') || '</td>');
          write_html('          </tr>');
        END LOOP;
        
        write_html('        </tbody>');
        write_html('      </table>');
        write_html('    </div>');
      END IF;
    END IF;
   write_html('  </div>');
    write_html('  <div class="footer">');
    write_html('    <div>МДиСУБД ЛР №5 © 2024</div>');
    write_html('    <div>Тимошевич Карина Сергеевна</div>');
    write_html('  </div>');
    write_html('</body>');
    write_html('</html>');
    UTL_FILE.FCLOSE(v_file);
    
    UPDATE report_tracking
    SET last_report_time = v_end_time,
        report_count = v_report_num;
    COMMIT;  
    DBMS_OUTPUT.PUT_LINE('Report #' || v_report_num || ' generated successfully at: ' || p_file_path);
    
  EXCEPTION
    WHEN OTHERS THEN
      IF UTL_FILE.IS_OPEN(v_file) THEN
        UTL_FILE.FCLOSE(v_file);
      END IF;
      DBMS_OUTPUT.PUT_LINE('Error generating report: ' || SQLERRM);
      RAISE;
  END;
END generate_changes_report;
/
