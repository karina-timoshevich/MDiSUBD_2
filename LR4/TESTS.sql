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

--тест селекта с нот ин
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name"],
    "tables": ["students"],
    "where": {
      "conditions": [
        {
          "column": "course_id",
          "operator": "NOT IN",
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

--тест селекта с exists
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name"],
    "tables": ["students"],
    "where": {
      "conditions": [
        {
          "column": "",
          "operator": "EXISTS",
          "subquery": {
            "columns": "course_id",
            "tables": "courses",
            "conditions": "instructor = ''Петрова М.И.'' AND course_id = students.course_id"
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

--тест селекта с not exists
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["first_name"],
    "tables": ["students"],
    "where": {
      "conditions": [
        {
          "column": "",
          "operator": "NOT EXISTS",
          "subquery": {
            "columns": "course_id",
            "tables": "courses",
            "conditions": "course_name = ''Физика'' AND course_id = students.course_id"
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

--тест на джоин с условием
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["students.first_name", "courses.course_name", "students.grade"],
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
      ]
    }
  }';
  v_cur SYS_REFCURSOR;
  v_name students.first_name%TYPE;
  v_course courses.course_name%TYPE;
  v_grade students.grade%TYPE;
BEGIN
  v_cur := json_select_handler(v_json);
  DBMS_OUTPUT.PUT_LINE('Студенты с оценкой >= 80:');
  LOOP
    FETCH v_cur INTO v_name, v_course, v_grade;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_name || ' | ' || v_course || ' | ' || v_grade);
  END LOOP;
  CLOSE v_cur;
END;
/

--INSERT INTO students (student_id, first_name, course_id, grade) VALUES (8, 'Семен', 2, 79);
--тест с груп бай
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["courses.course_name", "COUNT(students.student_id)"],
    "tables": ["courses"],
    "joins": [
      {
        "type": "LEFT JOIN",
        "table": "students",
        "on": "courses.course_id = students.course_id"
      }
    ],
    "group_by": ["courses.course_name"]
  }';
  v_cur SYS_REFCURSOR;
  v_course courses.course_name%TYPE;
  v_count NUMBER;
BEGIN
  v_cur := json_select_handler(v_json);
  DBMS_OUTPUT.PUT_LINE('Количество студентов по курсам:');
  LOOP
    FETCH v_cur INTO v_course, v_count;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_course || ' | ' || v_count);
  END LOOP;
  CLOSE v_cur;
END;
/

--тест с агрешатной функцией
DECLARE
  v_json CLOB := '{
    "operation": "SELECT",
    "columns": ["courses.course_name", "AVG(students.grade)"],
    "tables": ["courses"],
    "joins": [
      {
        "type": "INNER JOIN",
        "table": "students",
        "on": "courses.course_id = students.course_id"
      }
    ],
    "group_by": ["courses.course_name"]
  }';
  v_cur SYS_REFCURSOR;
  v_course courses.course_name%TYPE;
  v_avg NUMBER;
BEGIN
  v_cur := json_select_handler(v_json);
  DBMS_OUTPUT.PUT_LINE('Средняя оценка по курсам:');
  LOOP
    FETCH v_cur INTO v_course, v_avg;
    EXIT WHEN v_cur%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE(v_course || ' | ' || ROUND(v_avg, 1));
  END LOOP;
  CLOSE v_cur;
END;
/




