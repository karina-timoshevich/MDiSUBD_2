INSERT INTO departments (department_name, budget)
VALUES ('IT Department', 50000);

INSERT INTO departments (department_name, budget)
VALUES ('HR Department', 30000);

INSERT INTO employees (name, hire_date)
VALUES ('Alice Johnson', DATE '2021-01-15');

INSERT INTO employees (name, hire_date)
VALUES ('Bob Smith', DATE '2022-06-10');

INSERT INTO tasks (employee_id, task_name, due_date)
VALUES (1, 'Setup new servers', DATE '2024-12-01');

INSERT INTO tasks (employee_id, task_name, due_date)
VALUES (2, 'Prepare recruitment report', DATE '2024-11-10');

UPDATE employees
SET name = 'Alice Johnson-Smith'
WHERE employee_id = 1;

UPDATE departments
SET budget = 60000
WHERE department_id = 1;

UPDATE tasks
SET task_name = 'Setup production servers'
WHERE task_id = 1;

DELETE FROM tasks
WHERE task_id = 2;

DELETE FROM employees
WHERE employee_id = 2;

DELETE FROM departments
WHERE department_id = 2;

SELECT * FROM employees_history ORDER BY operation_time;

SELECT * FROM departments_history ORDER BY operation_time;

SELECT * FROM tasks_history ORDER BY operation_time;
