CREATE TABLE courses (
    course_id   NUMBER PRIMARY KEY,
    course_name VARCHAR2(100),
    instructor  VARCHAR2(50)
);

CREATE TABLE students (
    student_id  NUMBER PRIMARY KEY,
    first_name  VARCHAR2(50),
    course_id   NUMBER,
    grade       NUMBER,
    CONSTRAINT fk_course 
        FOREIGN KEY (course_id) 
        REFERENCES courses(course_id)
);

INSERT INTO courses (course_id, course_name, instructor) VALUES (1, 'Математика', 'Иванов А.А.');
INSERT INTO courses (course_id, course_name, instructor) VALUES (2, 'Физика', 'Петрова М.И.');

INSERT INTO students (student_id, first_name, course_id, grade) VALUES (1, 'Алексей', 1, 85);
INSERT INTO students (student_id, first_name, course_id, grade) VALUES (2, 'Мария', 2, 92);
INSERT INTO students (student_id, first_name, course_id, grade) VALUES (3, 'Дмитрий', 1, 78);
INSERT INTO students (student_id, first_name, course_id, grade) VALUES (5, 'Евдокия', 1, 85);
INSERT INTO students (student_id, first_name, course_id, grade) VALUES (6, 'Роман', 2, 87);
INSERT INTO students (student_id, first_name, course_id, grade) VALUES (7, 'Федя', 1, 64);
COMMIT;