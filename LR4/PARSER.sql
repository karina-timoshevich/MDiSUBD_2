CREATE OR REPLACE FUNCTION json_select_handler(p_json CLOB) RETURN SYS_REFCURSOR IS
  v_sql         VARCHAR2(4000);
  v_cur         SYS_REFCURSOR;
  v_columns     VARCHAR2(1000);
  v_tables      VARCHAR2(1000);
  v_join_clause VARCHAR2(1000) := '';
  v_where       VARCHAR2(1000) := '';
  v_logical_op  VARCHAR2(5) := 'AND';
BEGIN
  -- извлекаем колонки
  SELECT LISTAGG(column_name, ', ') 
  INTO v_columns
  FROM JSON_TABLE(p_json, '$.columns[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));

  -- тут таблицы
  SELECT LISTAGG(table_name, ', ') 
  INTO v_tables
  FROM JSON_TABLE(p_json, '$.tables[*]' COLUMNS (table_name VARCHAR2(50) PATH '$'));

  -- джоин формируем
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

  -- where формируем
  BEGIN
    SELECT ' WHERE ' || LISTAGG(
      condition_column || ' ' || condition_operator || ' ' || 
      CASE 
  WHEN REGEXP_LIKE(condition_value, '^\d+(\.\d+)?$') THEN condition_value 
  ELSE '''' || REPLACE(condition_value, '''', '''''') || '''' 
END, ' ' || v_logical_op || ' ') 
    INTO v_where
    FROM JSON_TABLE(p_json, '$.where.conditions[*]' 
           COLUMNS (
             condition_column VARCHAR2(50) PATH '$.column',
             condition_operator VARCHAR2(10) PATH '$.operator',
             condition_value VARCHAR2(100) PATH '$.value'
           ));
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