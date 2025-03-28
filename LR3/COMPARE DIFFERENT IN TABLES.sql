CREATE OR REPLACE PROCEDURE GENERATE_SYNC_SCRIPT(
    p_dev_schema VARCHAR2,
    p_prod_schema VARCHAR2
) AUTHID CURRENT_USER IS

    -- Улучшенная функция сравнения через DDL
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

    -- Indexes (special handling)
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '/* INDEX DIFFERENCES */');
    -- New indexes
    FOR idx IN (
        SELECT 
            i.index_name, 
            i.table_name, 
            i.uniqueness, 
            LISTAGG(c.column_name, ', ') WITHIN GROUP (ORDER BY c.column_position) AS idx_columns
        FROM all_indexes i
        JOIN all_ind_columns c 
            ON i.owner = c.index_owner 
            AND i.index_name = c.index_name
        WHERE i.owner = p_dev_schema
            AND i.index_name NOT LIKE '%_PK'
            AND i.index_name NOT IN (
                SELECT index_name 
                FROM all_indexes 
                WHERE owner = p_prod_schema
            )
        GROUP BY i.index_name, i.table_name, i.uniqueness
    ) LOOP
        DBMS_OUTPUT.PUT_LINE(
            'CREATE ' || idx.uniqueness || ' INDEX ' || p_prod_schema || '.' || idx.index_name ||
            ' ON ' || p_prod_schema || '.' || idx.table_name || '(' || idx.idx_columns || ');'
        );
    END LOOP;

    -- Obsolete indexes
    FOR idx IN (
        SELECT index_name
        FROM all_indexes
        WHERE owner = p_prod_schema
            AND index_name NOT LIKE '%_PK'
            AND index_name NOT IN (
                SELECT index_name 
                FROM all_indexes 
                WHERE owner = p_dev_schema
            )
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('DROP INDEX ' || p_prod_schema || '.' || idx.index_name || ';');
    END LOOP;

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