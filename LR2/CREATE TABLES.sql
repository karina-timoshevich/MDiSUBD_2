CREATE TABLE groups (
    group_id NUMBER NOT NULL,
    group_name VARCHAR2(20) NOT NULL,
    C_VAL NUMBER DEFAULT 0 NOT NULL
);

CREATE TABLE students (
    student_id NUMBER NOT NULL,
    student_name VARCHAR2(20) NOT NULL,
    group_id NUMBER NOT NULL
);