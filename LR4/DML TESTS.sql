
--тест простого инсерта
DECLARE
  v_json CLOB := '{
    "operation": "INSERT",
    "table": "students",
    "columns": ["student_id", "first_name", "course_id", "grade"],
    "values": ["4", "Никита", "2", "88"]
  }';
  v_result VARCHAR2(100);
BEGIN
  v_result := json_dml_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  FOR rec IN (SELECT student_id, first_name, course_id, grade FROM students ORDER BY student_id) LOOP
    DBMS_OUTPUT.PUT_LINE('ID: ' || rec.student_id || ', Имя: ' || rec.first_name || ', course_id: ' || rec.course_id || ', grade: ' || rec.grade);
  END LOOP;
END;
/

--тест апдейта
DECLARE
  v_json CLOB := '{
    "operation": "UPDATE",
    "table": "students",
    "set": [
      { "column": "grade", "value": "90" }
    ],
    "where": {
      "conditions": [
        {
          "column": "student_id",
          "operator": "=",
          "value": "4"
        }
      ],
      "logical_operator": "AND"
    }
  }';
  v_result VARCHAR2(100);
BEGIN
  v_result := json_dml_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  FOR rec IN (SELECT student_id, first_name, course_id, grade FROM students ORDER BY student_id) LOOP
    DBMS_OUTPUT.PUT_LINE('ID: ' || rec.student_id || ', Имя: ' || rec.first_name || ', course_id: ' || rec.course_id || ', grade: ' || rec.grade);
  END LOOP;
END;
/

--тест удаления
DECLARE
  v_json CLOB := '{
    "operation": "DELETE",
    "table": "students",
    "where": {
      "conditions": [
        {
          "column": "student_id",
          "operator": "=",
          "value": "4"
        }
      ],
      "logical_operator": "AND"
    }
  }';
  v_result VARCHAR2(100);
BEGIN
  v_result := json_dml_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  FOR rec IN (SELECT student_id, first_name, course_id, grade FROM students ORDER BY student_id) LOOP
    DBMS_OUTPUT.PUT_LINE('ID: ' || rec.student_id || ', Имя: ' || rec.first_name || ', course_id: ' || rec.course_id || ', grade: ' || rec.grade);
  END LOOP;
END;
/

--тест апдейта с подзапросом
DECLARE
  v_json CLOB := '{
    "operation": "UPDATE",
    "table": "students",
    "set": [
      { "column": "grade", "value": "99" }
    ],
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
  v_result VARCHAR2(100);
BEGIN
  v_result := json_dml_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  FOR rec IN (SELECT student_id, first_name, course_id, grade FROM students ORDER BY student_id) LOOP
    DBMS_OUTPUT.PUT_LINE('ID: ' || rec.student_id || ', Имя: ' || rec.first_name ||
                         ', course_id: ' || rec.course_id || ', grade: ' || rec.grade);
  END LOOP;
END;
/

--тест удаления с подзапросом
DECLARE
  v_json CLOB := '{
    "operation": "DELETE",
    "table": "students",
    "where": {
      "conditions": [
        {
          "column": "course_id",
          "operator": "IN",
          "subquery": {
            "columns": "course_id",
            "tables": "courses",
            "conditions": "course_name = ''Физика''"
          }
        }
      ],
      "logical_operator": "AND"
    }
  }';
  v_result VARCHAR2(100);
BEGIN
  v_result := json_dml_handler(v_json);
  DBMS_OUTPUT.PUT_LINE(v_result);
  
  FOR rec IN (SELECT student_id, first_name, course_id, grade FROM students ORDER BY student_id) LOOP
    DBMS_OUTPUT.PUT_LINE('ID: ' || rec.student_id || ', Имя: ' || rec.first_name ||
                         ', course_id: ' || rec.course_id || ', grade: ' || rec.grade);
  END LOOP;
END;
/
