CREATE OR REPLACE FUNCTION json_select_handler(p_json CLOB) RETURN SYS_REFCURSOR IS
  v_sql         VARCHAR2(4000);
  v_cur         SYS_REFCURSOR;
  v_columns     VARCHAR2(1000);
  v_tables      VARCHAR2(1000);
  v_join_clause VARCHAR2(1000) := '';
  v_where       VARCHAR2(4000) := '';
  v_group_by    VARCHAR2(1000) := '';
  v_logical_op  VARCHAR2(5) := 'AND';
BEGIN
  -- извлечение колонок
  SELECT LISTAGG(column_name, ', ') 
  INTO v_columns
  FROM JSON_TABLE(p_json, '$.columns[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));

  --извлечение таблиц
  SELECT LISTAGG(table_name, ', ') 
  INTO v_tables
  FROM JSON_TABLE(p_json, '$.tables[*]' COLUMNS (table_name VARCHAR2(50) PATH '$'));

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

  BEGIN
    SELECT LISTAGG(column_name, ', ')
    INTO v_group_by
    FROM JSON_TABLE(p_json, '$.group_by[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));
  EXCEPTION
    WHEN OTHERS THEN
      v_group_by := '';
  END;

  v_sql := 'SELECT ' || v_columns || 
           ' FROM ' || v_tables || 
           ' ' || v_join_clause || 
           v_where ||
           CASE WHEN v_group_by IS NOT NULL THEN ' GROUP BY ' || v_group_by ELSE '' END;
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
  v_sql           VARCHAR2(4000);
  v_op            VARCHAR2(10);
  v_result        VARCHAR2(200);
  v_table         VARCHAR2(50);
  v_columns       VARCHAR2(2000) := '';
  v_foreign_keys  VARCHAR2(2000) := '';
  v_primary_col   VARCHAR2(50);
BEGIN
  SELECT operation INTO v_op
  FROM JSON_TABLE(p_json, '$' COLUMNS (operation VARCHAR2(10) PATH '$.operation'));

  IF UPPER(v_op) = 'CREATE' THEN
    -- Извлечение имени таблицы
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(50) PATH '$.table'));

    -- Формирование колонок
    SELECT LISTAGG(
      column_name || ' ' || data_type || ' ' || NVL(constraints, ''),
      ', '
    ) INTO v_columns
    FROM JSON_TABLE(p_json, '$.columns[*]' 
      COLUMNS (
        column_name VARCHAR2(50) PATH '$.name',
        data_type   VARCHAR2(50) PATH '$.type',
        constraints VARCHAR2(100) PATH '$.constraints'
      )
    );

    -- Формирование FOREIGN KEYS
    SELECT LISTAGG(
      'CONSTRAINT fk_' || column_name || 
      ' FOREIGN KEY (' || column_name || ') REFERENCES ' ||
      ref_table || '(' || ref_column || ')',
      ', '
    ) INTO v_foreign_keys
    FROM JSON_TABLE(p_json, '$.foreign_keys[*]'
      COLUMNS (
        column_name VARCHAR2(50) PATH '$.column',
        ref_table   VARCHAR2(50) PATH '$.references.table',
        ref_column  VARCHAR2(50) PATH '$.references.column'
      )
    );

    -- Сборка полного SQL
    v_sql := 'CREATE TABLE ' || v_table || ' (' || v_columns;
    IF v_foreign_keys IS NOT NULL THEN
      v_sql := v_sql || ', ' || v_foreign_keys;
    END IF;
    v_sql := v_sql || ')';

    EXECUTE IMMEDIATE v_sql;

    -- Поиск первичного ключа для триггера
    BEGIN
      SELECT column_name INTO v_primary_col
      FROM JSON_TABLE(p_json, '$.columns[*]'
        COLUMNS (
          column_name VARCHAR2(50) PATH '$.name',
          constraints VARCHAR2(100) PATH '$.constraints'
        )
      )
      WHERE UPPER(constraints) LIKE '%PRIMARY%KEY%' AND ROWNUM = 1;

      IF v_primary_col IS NOT NULL THEN
        EXECUTE IMMEDIATE 'CREATE SEQUENCE ' || v_table || '_seq START WITH 1 INCREMENT BY 1';
        
        v_sql := 'CREATE OR REPLACE TRIGGER trg_' || v_table || 
                 ' BEFORE INSERT ON ' || v_table ||
                 ' FOR EACH ROW BEGIN ' ||
                 '   IF :NEW.' || v_primary_col || ' IS NULL THEN ' ||
                 '     SELECT ' || v_table || '_seq.NEXTVAL INTO :NEW.' || v_primary_col || ' FROM DUAL;' ||
                 '   END IF; END;';
        EXECUTE IMMEDIATE v_sql;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN NULL;
    END;

    v_result := 'Table ' || v_table || ' created';
    
  ELSIF UPPER(v_op) = 'DROP' THEN
    -- Удаление с каскадом
    SELECT table_name INTO v_table
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(50) PATH '$.table'));

    BEGIN
      EXECUTE IMMEDIATE 'DROP TRIGGER trg_' || v_table;
    EXCEPTION WHEN OTHERS THEN NULL; END;

    BEGIN
      EXECUTE IMMEDIATE 'DROP SEQUENCE ' || v_table || '_seq';
    EXCEPTION WHEN OTHERS THEN NULL; END;

    EXECUTE IMMEDIATE 'DROP TABLE ' || v_table || ' CASCADE CONSTRAINTS';
    v_result := 'Table ' || v_table || ' dropped';
  END IF;

  RETURN v_result;
EXCEPTION
  WHEN OTHERS THEN
    RETURN 'Error: ' || SQLERRM || ' (SQL: ' || v_sql || ')';
END;
/