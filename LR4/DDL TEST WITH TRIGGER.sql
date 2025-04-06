DECLARE
  v_result VARCHAR2(4000);
BEGIN
  v_result := json_ddl_handler('{
    "operation": "CREATE",
    "table": "DEP",
    "columns": [
      {"name": "DEPT_ID", "type": "NUMBER", "constraints": "PRIMARY KEY"},
      {"name": "DEPT_NAME", "type": "VARCHAR2(100)"}
    ]
  }');
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  EXECUTE IMMEDIATE 'INSERT INTO DEP (DEPT_ID, DEPT_NAME) VALUES (1, ''IT Department'')';
  COMMIT;
END;
/

DECLARE
  v_result VARCHAR2(4000);
BEGIN
  v_result := json_ddl_handler('{
    "operation": "CREATE",
    "table": "EMP",
    "columns": [
      {"name": "EMP_ID", "type": "NUMBER", "constraints": "PRIMARY KEY"},
      {"name": "NAME", "type": "VARCHAR2(100)"},
      {"name": "DEPT_ID", "type": "NUMBER"}
    ],
    "foreign_keys": [
      {
        "column": "DEPT_ID",
        "references": {"table": "DEP", "column": "DEPT_ID"}
      }
    ]
  }');
  DBMS_OUTPUT.PUT_LINE(v_result);
END;
/

DECLARE
  v_id NUMBER;
BEGIN
  INSERT INTO EMP (NAME, DEPT_ID) VALUES ('First chel', 1);
  SELECT EMP_ID INTO v_id FROM EMP WHERE NAME = 'First chel';
  DBMS_OUTPUT.PUT_LINE('Сгенерированный EMP_ID: ' || v_id);
  COMMIT;
  INSERT INTO EMP (NAME, DEPT_ID) VALUES ('Second chel', 1);
  SELECT EMP_ID INTO v_id FROM EMP WHERE NAME = 'Second chel';
  DBMS_OUTPUT.PUT_LINE('Сгенерированный EMP_ID: ' || v_id);
  COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;
/

BEGIN
  DBMS_OUTPUT.PUT_LINE(json_ddl_handler('{"operation":"DROP","table":"EMP"}'));
  DBMS_OUTPUT.PUT_LINE(json_ddl_handler('{"operation":"DROP","table":"DEP"}'));
END;
/