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