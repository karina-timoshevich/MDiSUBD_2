CREATE OR REPLACE FUNCTION calculate_annual_reward (
    p_monthly_salary IN NUMBER, 
    p_bonus_percentage IN NUMBER
) 
RETURN NUMBER IS
    annual_reward NUMBER;
BEGIN
    IF p_monthly_salary <= 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Месячная зарплата должна быть положительным числом.');
    END IF;

    IF p_bonus_percentage < 0 THEN
        RAISE_APPLICATION_ERROR(-20002, 'Процент годовых премиальных не может быть отрицательным.');
    END IF;
    
    IF p_bonus_percentage <> TRUNC(p_bonus_percentage) THEN
        RAISE_APPLICATION_ERROR(-20004, 'Процент годовых премиальных должен быть целым числом.');
    END IF;

    annual_reward := (1 + p_bonus_percentage / 100) * 12 * p_monthly_salary;
    
    RETURN annual_reward;

EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20003, 'Произошла ошибка: ' || SQLERRM);
END calculate_annual_reward;
/
SELECT calculate_annual_reward(50000, 20) FROM dual;
SELECT calculate_annual_reward(50000, 20.5) FROM dual; 
