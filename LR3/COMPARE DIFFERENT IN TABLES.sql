CREATE OR REPLACE PROCEDURE GENERATE_SYNC_SCRIPT(
    p_dev_schema VARCHAR2,
    p_prod_schema VARCHAR2
) AUTHID CURRENT_USER IS

    -- Функция для улучшенного сравнения через DDL
    FUNCTION OBJECTS_DIFFERENT(
        p_object_name VARCHAR2,
        p_object_type VARCHAR2
    ) RETURN BOOLEAN IS
        v_dev_ddl  CLOB;
        v_prod_ddl CLOB;
    BEGIN
        BEGIN
            SELECT DBMS_METADATA.GET_DDL(p_object_type, p_object_name, p_dev_schema)
            INTO v_dev_ddl
            FROM DUAL;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN TRUE;
        END;

        BEGIN
            SELECT DBMS_METADATA.GET_DDL(p_object_type, p_object_name, p_prod_schema)
            INTO v_prod_ddl
            FROM DUAL;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN TRUE;
        END;

        -- Удаляем ключевые слова, такие как 'EDITABLE', для сравнения
        v_dev_ddl := REPLACE(v_dev_ddl, 'EDITIONABLE ', '');
        v_prod_ddl := REPLACE(v_prod_ddl, 'EDITIONABLE ', '');

        RETURN v_dev_ddl <> v_prod_ddl;
    END;

    -- Процедура для обработки объектов
    PROCEDURE PROCESS_OBJECTS(
        p_object_type VARCHAR2
    ) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || '/* ' || p_object_type || ' DIFFERENCES */');
        
        -- New objects
        FOR obj IN (
            SELECT object_name
            FROM all_objects
            WHERE owner = p_dev_schema
                AND object_type = p_object_type
                AND object_name NOT IN (
                    SELECT object_name 
                    FROM all_objects 
                    WHERE owner = p_prod_schema 
                        AND object_type = p_object_type
                )
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('-- Create ' || p_object_type || ': ' || obj.object_name);
            DBMS_OUTPUT.PUT_LINE(
                REPLACE(
                    DBMS_METADATA.GET_DDL(p_object_type, obj.object_name, p_dev_schema),
                    '"' || p_dev_schema || '"',
                    '"' || p_prod_schema || '"'
                ) || '/'
            );
        END LOOP;

        -- Changed objects
        FOR obj IN (
            SELECT object_name
            FROM all_objects
            WHERE owner = p_dev_schema
                AND object_type = p_object_type
                AND object_name IN (
                    SELECT object_name 
                    FROM all_objects 
                    WHERE owner = p_prod_schema 
                        AND object_type = p_object_type
                )
        ) LOOP
            IF OBJECTS_DIFFERENT(obj.object_name, p_object_type) THEN
                DBMS_OUTPUT.PUT_LINE('-- Update ' || p_object_type || ': ' || obj.object_name);
                DBMS_OUTPUT.PUT_LINE(
                    REPLACE(
                        DBMS_METADATA.GET_DDL(p_object_type, obj.object_name, p_dev_schema),
                        '"' || p_dev_schema || '"',
                        '"' || p_prod_schema || '"'
                    ) || '/'
                );
            END IF;
        END LOOP;

        -- Obsolete objects
        FOR obj IN (
            SELECT object_name
            FROM all_objects
            WHERE owner = p_prod_schema
                AND object_type = p_object_type
                AND object_name NOT IN (
                    SELECT object_name 
                    FROM all_objects 
                    WHERE owner = p_dev_schema 
                        AND object_type = p_object_type
                )
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('DROP ' || p_object_type || ' ' || p_prod_schema || '.' || obj.object_name || ';');
        END LOOP;
    END;

BEGIN
    DBMS_OUTPUT.PUT_LINE('-- Schema synchronization script');
    DBMS_OUTPUT.PUT_LINE('-- Generated: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('-- Source schema: ' || p_dev_schema);
    DBMS_OUTPUT.PUT_LINE('-- Target schema: ' || p_prod_schema);
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------------');

    -- Process other object types
    PROCESS_OBJECTS('PROCEDURE');
    PROCESS_OBJECTS('FUNCTION');

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '-- End of synchronization script');
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------------');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error generating script: ' || SQLERRM);
END;
/
