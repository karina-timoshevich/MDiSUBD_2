CREATE OR REPLACE FUNCTION json_select_handler(p_json CLOB) RETURN SYS_REFCURSOR IS
  v_sql         VARCHAR2(4000);
  v_cur         SYS_REFCURSOR;
  v_columns     VARCHAR2(1000);
  v_tables      VARCHAR2(1000);
  v_join_clause VARCHAR2(1000) := '';
  v_where       VARCHAR2(4000) := '';
  v_logical_op  VARCHAR2(5) := 'AND';
BEGIN
  -- извлекаем колонки
  SELECT LISTAGG(column_name, ', ') 
  INTO v_columns
  FROM JSON_TABLE(p_json, '$.columns[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));

  -- извлекаем таблицы
  SELECT LISTAGG(table_name, ', ') 
  INTO v_tables
  FROM JSON_TABLE(p_json, '$.tables[*]' COLUMNS (table_name VARCHAR2(50) PATH '$'));

  -- формируем джоин
  BEGIN
    SELECT LISTAGG(jt.join_type || ' ' || jt.join_table || ' ON ' || jt.join_condition, ' ') 
    INTO v_join_clause
    FROM JSON_TABLE(p_json, '$.joins[*]' 
           COLUMNS (
             join_type VARCHAR2(20) PATH '$.type',
             join_table VARCHAR2(50) PATH '$.table',
             join_condition VARCHAR2(200) PATH '$.on'
           )) jt;
  EXCEPTION
    WHEN OTHERS THEN
      v_join_clause := '';
  END;

  -- формируем where с подзапросами
  BEGIN
    FOR cond IN (
      SELECT *
      FROM JSON_TABLE(p_json, '$.where.conditions[*]'
        COLUMNS (
          condition_column     VARCHAR2(100) PATH '$.column',
          condition_operator   VARCHAR2(20)  PATH '$.operator',
          condition_value      VARCHAR2(100) PATH '$.value',
          subquery_columns     CLOB          PATH '$.subquery.columns',
          subquery_tables      CLOB          PATH '$.subquery.tables',
          subquery_conditions  CLOB          PATH '$.subquery.conditions'
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

  v_sql := 'SELECT ' || v_columns || ' FROM ' || v_tables || ' ' || v_join_clause || v_where;

  OPEN v_cur FOR v_sql;
  RETURN v_cur;
EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20001, 'Ошибка формирования запроса: ' || SQLERRM || '. SQL: ' || v_sql);
END;
/

CREATE OR REPLACE FUNCTION json_dml_handler(p_json CLOB) RETURN VARCHAR2 IS
  v_sql         VARCHAR2(4000);
  v_op          VARCHAR2(10);
  v_result      VARCHAR2(100);
  v_count       NUMBER;
  v_table       VARCHAR2(50);
  v_columns     VARCHAR2(1000);
  v_values      VARCHAR2(1000);
  v_set_clause  VARCHAR2(1000);
  v_where       VARCHAR2(4000) := '';
  v_logical_op  VARCHAR2(5) := 'AND';
BEGIN
  SELECT operation INTO v_op
  FROM JSON_TABLE(p_json, '$'
       COLUMNS (
         operation VARCHAR2(10) PATH '$.operation'
       )
  );

  IF UPPER(v_op) = 'INSERT' THEN
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$'
         COLUMNS (
           table_name VARCHAR2(50) PATH '$.table'
         )
    );
    
    SELECT LISTAGG(column_name, ', ') 
    INTO v_columns
    FROM JSON_TABLE(p_json, '$.columns[*]'
         COLUMNS (column_name VARCHAR2(100) PATH '$')
    );
    
    SELECT LISTAGG(
             CASE 
               WHEN REGEXP_LIKE(value, '^\d+(\.\d+)?$') THEN value
               ELSE '''' || REPLACE(value, '''', '''''') || ''''
             END, ', ')
    INTO v_values
    FROM JSON_TABLE(p_json, '$.values[*]'
         COLUMNS (value VARCHAR2(100) PATH '$')
    );
    
    v_sql := 'INSERT INTO ' || v_table || ' (' || v_columns || ') VALUES (' || v_values || ')';
    
    EXECUTE IMMEDIATE v_sql;
    v_count := SQL%ROWCOUNT;
    v_result := 'Rows inserted: ' || v_count;

  ELSIF UPPER(v_op) = 'UPDATE' THEN
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$'
         COLUMNS (
           table_name VARCHAR2(50) PATH '$.table'
         )
    );
    
    SELECT LISTAGG(column_name || ' = ' ||
           CASE 
             WHEN REGEXP_LIKE(value, '^\d+(\.\d+)?$') THEN value
             ELSE '''' || REPLACE(value, '''', '''''') || ''''
           END, ', ')
    INTO v_set_clause
    FROM JSON_TABLE(p_json, '$.set[*]'
         COLUMNS (
           column_name VARCHAR2(100) PATH '$.column',
           value       VARCHAR2(100) PATH '$.value'
         )
    );
    
    BEGIN
      FOR cond IN (
        SELECT *
        FROM JSON_TABLE(p_json, '$.where.conditions[*]'
          COLUMNS (
            condition_column     VARCHAR2(100) PATH '$.column',
            condition_operator   VARCHAR2(20)  PATH '$.operator',
            condition_value      VARCHAR2(100) PATH '$.value',
            subquery_columns     CLOB          PATH '$.subquery.columns',
            subquery_tables      CLOB          PATH '$.subquery.tables',
            subquery_conditions  CLOB          PATH '$.subquery.conditions'
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
    
    v_sql := 'UPDATE ' || v_table || ' SET ' || v_set_clause || v_where;
    
    EXECUTE IMMEDIATE v_sql;
    v_count := SQL%ROWCOUNT;
    v_result := 'Rows updated: ' || v_count;

  ELSIF UPPER(v_op) = 'DELETE' THEN
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$'
         COLUMNS (
           table_name VARCHAR2(50) PATH '$.table'
         )
    );
    
    BEGIN
      FOR cond IN (
        SELECT *
        FROM JSON_TABLE(p_json, '$.where.conditions[*]'
          COLUMNS (
            condition_column     VARCHAR2(100) PATH '$.column',
            condition_operator   VARCHAR2(20)  PATH '$.operator',
            condition_value      VARCHAR2(100) PATH '$.value',
            subquery_columns     CLOB          PATH '$.subquery.columns',
            subquery_tables      CLOB          PATH '$.subquery.tables',
            subquery_conditions  CLOB          PATH '$.subquery.conditions'
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
    
    v_sql := 'DELETE FROM ' || v_table || v_where;
    
    EXECUTE IMMEDIATE v_sql;
    v_count := SQL%ROWCOUNT;
    v_result := 'Rows deleted: ' || v_count;

  ELSE
    v_result := 'Unsupported operation: ' || v_op;
  END IF;

  RETURN v_result;

EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20002, 'Ошибка формирования/выполнения запроса: ' || SQLERRM || '. SQL: ' || v_sql);
END;
/

CREATE OR REPLACE FUNCTION json_ddl_handler(p_json CLOB) RETURN VARCHAR2 IS
  v_sql      VARCHAR2(4000);
  v_op       VARCHAR2(10);
  v_result   VARCHAR2(200);
  v_table    VARCHAR2(50);
  v_columns  VARCHAR2(2000) := '';
BEGIN
  SELECT operation INTO v_op
  FROM JSON_TABLE(p_json, '$'
       COLUMNS (
         operation VARCHAR2(10) PATH '$.operation'
       )
  );
  
  IF UPPER(v_op) = 'CREATE' THEN
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$'
         COLUMNS (
           table_name VARCHAR2(50) PATH '$.table'
         )
    );
    
    SELECT LISTAGG(col_definition, ', ') WITHIN GROUP (ORDER BY rn)
      INTO v_columns
    FROM (
      SELECT ROWNUM rn,
             column_name || ' ' || data_type AS col_definition
      FROM JSON_TABLE(p_json, '$.columns[*]'
           COLUMNS (
             column_name VARCHAR2(50) PATH '$.name',
             data_type   VARCHAR2(50) PATH '$.type'
           )
      )
    );
    
    v_sql := 'CREATE TABLE ' || v_table || ' (' || v_columns || ')';
    
    EXECUTE IMMEDIATE v_sql;
    v_result := 'Table "' || v_table || '" created successfully.';

  ELSIF UPPER(v_op) = 'DROP' THEN
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$'
         COLUMNS (
           table_name VARCHAR2(50) PATH '$.table'
         )
    );
    
    v_sql := 'DROP TABLE ' || v_table;
    EXECUTE IMMEDIATE v_sql;
    v_result := 'Table "' || v_table || '" dropped successfully.';
  ELSE
    v_result := 'Unsupported DDL operation: ' || v_op;
  END IF;
  
  RETURN v_result;
  
EXCEPTION
  WHEN OTHERS THEN
    RAISE_APPLICATION_ERROR(-20003, 'Ошибка DDL: ' || SQLERRM || '. SQL: ' || v_sql);
END;
/
