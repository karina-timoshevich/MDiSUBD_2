CREATE OR REPLACE FUNCTION compare_even_odd_count RETURN VARCHAR2 IS
    even_count NUMBER := 0;
    odd_count NUMBER := 0;
BEGIN
    FOR rec IN (SELECT val FROM MyTable) LOOP
        IF MOD(rec.val, 2) = 0 THEN
            even_count := even_count + 1;
        ELSE
            odd_count := odd_count + 1;
        END IF;
    END LOOP;

    IF even_count > odd_count THEN
        RETURN 'TRUE';
    ELSIF odd_count > even_count THEN
        RETURN 'FALSE';
    ELSE
        RETURN 'EQUAL';
    END IF;
END compare_even_odd_count;

/*
SELECT compare_even_odd_count FROM dual;
SELECT COUNT(*) 
FROM MyTable 
WHERE MOD(val, 2) = 1;
*/
