CREATE OR REPLACE PROCEDURE insert_into_mytable (p_id IN NUMBER, p_val IN NUMBER) IS
BEGIN
    INSERT INTO MyTable (id, val)
    VALUES (p_id, p_val);
    
    COMMIT; 
END insert_into_mytable;
/

CREATE OR REPLACE PROCEDURE insert_into_mytable_inc (p_val IN NUMBER) IS
    v_new_id NUMBER;
BEGIN
    SELECT NVL(MAX(id), 0) + 1 INTO v_new_id FROM MyTable;
    INSERT INTO MyTable (id, val)
    VALUES (v_new_id, p_val);
    COMMIT;
END insert_into_mytable_inc;
/
--EXEC insert_into_mytable(10002, 45);
select val from MYTABLE where id = 10002;

CREATE OR REPLACE PROCEDURE update_mytable (p_id IN NUMBER, p_new_val IN NUMBER) IS
BEGIN
    UPDATE MyTable
    SET val = p_new_val
    WHERE id = p_id;
    
    COMMIT; 
END update_mytable;
/

EXEC update_mytable(10002, 55);
select val from MYTABLE where id = 10002;

CREATE OR REPLACE PROCEDURE delete_from_mytable (p_id IN NUMBER) IS
BEGIN
    DELETE FROM MyTable
    WHERE id = p_id;
    
    COMMIT; 
END delete_from_mytable;
/

EXEC delete_from_mytable(10002);
select val from MYTABLE where id = 10002;

