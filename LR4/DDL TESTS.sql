DECLARE
  v_json CLOB := '{
    "operation": "CREATE",
    "table": "test_table",
    "columns": [
      { "name": "id", "type": "NUMBER PRIMARY KEY" },
      { "name": "name", "type": "VARCHAR2(100)" },
      { "name": "created_at", "type": "DATE" }
    ]
  }';
  v_result VARCHAR2(200);
BEGIN
  v_result := json_ddl_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  FOR rec IN (
    SELECT table_name, column_name, data_type
    FROM user_tab_columns
    WHERE table_name = 'TEST_TABLE'
    ORDER BY column_id
  ) LOOP
    DBMS_OUTPUT.PUT_LINE('Table: ' || rec.table_name || ', Column: ' || rec.column_name || ', Type: ' || rec.data_type);
  END LOOP;
END;
/

DECLARE
  v_json CLOB := '{
    "operation": "DROP",
    "table": "test_table"
  }';
  v_result VARCHAR2(200);
BEGIN
  v_result := json_ddl_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  -- проверка, что таблица удалена черех выборку из USER_TABLES
  DECLARE
    v_dummy NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_dummy FROM user_tables WHERE table_name = 'TEST_TABLE';
    DBMS_OUTPUT.PUT_LINE('Remaining tables with name TEST_TABLE: ' || v_dummy);
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error checking table existence: ' || SQLERRM);
  END;
END;
/
