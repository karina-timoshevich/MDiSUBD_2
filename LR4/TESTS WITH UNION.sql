CREATE OR REPLACE FUNCTION process_select_part(p_query CLOB) RETURN VARCHAR2 IS
  v_columns      VARCHAR2(1000);
  v_tables       VARCHAR2(1000);
  v_join_clause  VARCHAR2(1000) := '';
  v_where        VARCHAR2(4000) := '';
  v_group_by     VARCHAR2(1000) := '';
  v_sql          VARCHAR2(4000);
  v_logical_op   VARCHAR2(5) := 'AND';
BEGIN
  SELECT LISTAGG(column_name, ', ') 
    INTO v_columns
  FROM JSON_TABLE(TO_CHAR(p_query), '$.columns[*]'
       COLUMNS (column_name VARCHAR2(100) PATH '$'));
  SELECT LISTAGG(table_name, ', ') WITHIN GROUP (ORDER BY table_name)
    INTO v_tables
  FROM JSON_TABLE(TO_CHAR(p_query), '$.tables[*]'
       COLUMNS (table_name VARCHAR2(50) PATH '$'));

  BEGIN
    SELECT LISTAGG(jt.join_type || ' ' || jt.join_table || ' ON ' || jt.join_condition, ' ')
      INTO v_join_clause
    FROM JSON_TABLE(TO_CHAR(p_query), '$.joins[*]'
           COLUMNS (
             join_type      VARCHAR2(20) PATH '$.type',
             join_table     VARCHAR2(50) PATH '$.table',
             join_condition VARCHAR2(200) PATH '$.on'
           )) jt;
  EXCEPTION
    WHEN OTHERS THEN
      v_join_clause := '';
  END;
  
  BEGIN
    FOR cond IN (
      SELECT *
      FROM JSON_TABLE(TO_CHAR(p_query), '$.where.conditions[*]'
        COLUMNS (
          condition_column     VARCHAR2(100) PATH '$.column',
          condition_operator   VARCHAR2(20)  PATH '$.operator',
          condition_value      VARCHAR2(100) PATH '$.value',
          subquery_columns     VARCHAR2(4000) PATH '$.subquery.columns',
          subquery_tables      VARCHAR2(4000) PATH '$.subquery.tables',
          subquery_conditions  VARCHAR2(4000) PATH '$.subquery.conditions'
        )
      )
    ) LOOP
      IF cond.subquery_columns IS NOT NULL AND cond.subquery_tables IS NOT NULL THEN
        DECLARE
          v_subquery VARCHAR2(1000);
        BEGIN
          v_subquery := '(SELECT ' ||
                        RTRIM(REPLACE(REPLACE(cond.subquery_columns, '["', ''), '"]', ''), '"') ||
                        ' FROM ' || RTRIM(REPLACE(REPLACE(cond.subquery_tables, '["', ''), '"]', ''), '"');
          IF cond.subquery_conditions IS NOT NULL THEN
            v_subquery := v_subquery || ' WHERE ' ||
                          RTRIM(REPLACE(REPLACE(cond.subquery_conditions, '["', ''), '"]', ''), '"');
          END IF;
          v_subquery := v_subquery || ')';
          v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' || v_subquery || ' ' || v_logical_op || ' ';
        END;
      ELSE
        v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' ||
          CASE 
            WHEN REGEXP_LIKE(cond.condition_value, '^\d+(\.\d+)?$') THEN cond.condition_value
            ELSE '''' || REPLACE(cond.condition_value, '''', '''''') || ''''
          END || ' ' || v_logical_op || ' ';
      END IF;
    END LOOP;
    IF v_where IS NOT NULL THEN
      v_where := ' WHERE ' || RTRIM(v_where, ' ' || v_logical_op || ' ');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      v_where := '';
  END;
  BEGIN
    SELECT LISTAGG(column_name, ', ')
      INTO v_group_by
    FROM JSON_TABLE(TO_CHAR(p_query), '$.group_by[*]'
         COLUMNS (column_name VARCHAR2(100) PATH '$'));
  EXCEPTION
    WHEN OTHERS THEN
      v_group_by := '';
  END;

  v_sql := 'SELECT ' || v_columns ||
           ' FROM ' || v_tables ||
           ' ' || v_join_clause ||
           v_where ||
           CASE WHEN v_group_by IS NOT NULL AND v_group_by <> '' THEN ' GROUP BY ' || v_group_by ELSE '' END;
  
  RETURN v_sql;
END;
/


CREATE OR REPLACE FUNCTION json_select_handler_union(p_json CLOB) RETURN SYS_REFCURSOR IS
  v_sql             VARCHAR2(4000);
  v_cur             SYS_REFCURSOR;
  v_union_type      VARCHAR2(10) := 'UNION'; 
  v_union_all_flag  NUMBER := 0;              
BEGIN
  v_sql := process_select_part(p_json);
  
  BEGIN
    SELECT NVL(union_type, 'UNION'),
           NVL(union_all, 0)
      INTO v_union_type,
           v_union_all_flag
      FROM JSON_TABLE(TO_CHAR(p_json), '$.union'
           COLUMNS (
             union_type VARCHAR2(10) PATH '$.type',
             union_all  NUMBER PATH '$.all'
           ));
    IF v_union_all_flag = 1 THEN
      v_union_type := 'UNION ALL';
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL; 
  END;
  
  FOR u IN (
    SELECT JSON_SERIALIZE(jt.query RETURNING VARCHAR2(4000)) AS query_str,
           jt.all_flag
      FROM JSON_TABLE(TO_CHAR(p_json), '$.union.queries[*]'
           COLUMNS (
             query    FORMAT JSON PATH '$',
             all_flag NUMBER PATH '$.all'
           )) jt
  ) LOOP
    v_sql := v_sql || ' ' || v_union_type ||
             CASE WHEN u.all_flag = 1 THEN ' ALL' ELSE '' END || ' ' ||
             process_select_part(u.query_str);
  END LOOP;
  
  OPEN v_cur FOR v_sql;
  RETURN v_cur;
EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20001, 'Ошибка формирования запроса: ' || SQLERRM || '. SQL: ' || v_sql);
END;
/

--тест юнион имен студентов и преподавателей (уникальные значения)
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name"],
    "tables": ["students"],
    "union": {
      "queries": [
        {
          "columns": ["instructor"],
          "tables": ["courses"]
        }
      ]
    }
  }';
  v_cur SYS_REFCURSOR;
  v_name VARCHAR2(50);
BEGIN
  v_cur := json_select_handler_union(v_json);
  DBMS_OUTPUT.PUT_LINE('Результат UNION:');
  LOOP
    FETCH v_cur INTO v_name;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_name);
  END LOOP;
  CLOSE v_cur;
END;
/

--тест юнион олл с дубликатами (все записи)
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name"],
    "tables": ["students"],
    "union": {
      "type": "UNION ALL",
      "queries": [
        {
          "columns": ["instructor"],
          "tables": ["courses"]
        },
        {
          "columns": ["first_name"],
          "tables": ["students"],
          "where": {
            "conditions": [{"column": "grade", "operator": ">", "value": "80"}]
          }
        }
      ]
    }
  }';
  v_cur SYS_REFCURSOR;
  v_name VARCHAR2(50);
BEGIN
  v_cur := json_select_handler_union(v_json);
  DBMS_OUTPUT.PUT_LINE('UNION ALL с дубликатами:');
  LOOP
    FETCH v_cur INTO v_name;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_name);
  END LOOP;
  CLOSE v_cur;
END;
/

--тест юнион с разными столбцами 
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name", "TO_CHAR(grade)"],
    "tables": ["students"],
    "union": {
      "queries": [
        {
          "columns": ["instructor", "course_name"],
          "tables": ["courses"]
        }
      ]
    }
  }';
  v_cur SYS_REFCURSOR;
  v_col1 VARCHAR2(50);
  v_col2 VARCHAR2(100);
BEGIN
  v_cur := json_select_handler_union(v_json);
  DBMS_OUTPUT.PUT_LINE('UNION с разными типами данных:');
  LOOP
    FETCH v_cur INTO v_col1, v_col2;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_col1 || ' | ' || v_col2);
  END LOOP;
  CLOSE v_cur;
END;
/

select * from STUDENTS;
select * from COURSES;

--тест юнион с условиями where
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name", "grade"],
    "tables": ["students"],
    "where": {
      "conditions": [
        {"column": "grade", "operator": ">=", "value": "80"}
      ]
    },
    "union": {
      "queries": [
        {
          "columns": ["instructor", "course_id"],
          "tables": ["courses"],
          "where": {
            "conditions": [
              {"column": "instructor", "operator": "LIKE", "value": "%Иванов%"}
            ]
          }
        }
      ]
    }
  }';
  v_cur SYS_REFCURSOR;
  v_col1 VARCHAR2(50);
  v_col2 VARCHAR2(100);
BEGIN
  v_cur := json_select_handler_union(v_json);
  DBMS_OUTPUT.PUT_LINE('UNION с условиями WHERE:');
  LOOP
    FETCH v_cur INTO v_col1, v_col2;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_col1 || ' | ' || v_col2);
  END LOOP;
  CLOSE v_cur;
END;
/
