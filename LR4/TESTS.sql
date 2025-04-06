--тест селекта с джоином
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["students.first_name", "courses.course_name"],
    "tables": ["students"],
    "joins": [
      {
        "type": "INNER JOIN",
        "table": "courses",
        "on": "students.course_id = courses.course_id"
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "students.grade",
          "operator": ">=",
          "value": "80"
        }
      ],
      "logical_operator": "AND"
    }
  }';
  v_cur SYS_REFCURSOR;
  v_name students.first_name%TYPE;
  v_course courses.course_name%TYPE;
BEGIN
  v_cur := json_select_handler(v_json);
  LOOP
    FETCH v_cur INTO v_name, v_course;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_name || ' | ' || v_course);
  END LOOP;
  CLOSE v_cur;
END;
/

-- тест where с подхапросом
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name"],
    "tables": ["students"],
    "where": {
      "conditions": [
        {
          "column": "course_id",
          "operator": "IN",
          "subquery": {
            "columns": "course_id",
            "tables": "courses",
            "conditions": "instructor = ''Иванов А.А.''"
          }
        }
      ],
      "logical_operator": "AND"
    }
  }';

  v_cur SYS_REFCURSOR;
  v_name students.first_name%TYPE;
BEGIN
  v_cur := json_select_handler(v_json);
  LOOP
    FETCH v_cur INTO v_name;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_name);
  END LOOP;
  CLOSE v_cur;
END;
/

--тест селекта с джоином и подзапросом
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["students.first_name", "courses.course_name"],
    "tables": ["students"],
    "joins": [
      {
        "type": "INNER JOIN",
        "table": "courses",
        "on": "students.course_id = courses.course_id"
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "students.grade",
          "operator": ">=",
          "value": "80"
        },
        {
          "column": "students.course_id",
          "operator": "IN",
          "subquery": {
            "columns": "course_id",
            "tables": "courses",
            "conditions": "instructor = ''Иванов А.А.''"
          }
        }
      ],
      "logical_operator": "AND"
    }
  }';
  v_cur SYS_REFCURSOR;
  v_name students.first_name%TYPE;
  v_course courses.course_name%TYPE;
BEGIN
  v_cur := json_select_handler(v_json);
  LOOP
    FETCH v_cur INTO v_name, v_course;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_name || ' | ' || v_course);
  END LOOP;
  CLOSE v_cur;
END;
/
