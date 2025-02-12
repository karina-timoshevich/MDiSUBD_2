SELECT * FROM v$transaction;
SELECT s.sid, s.serial#, s.username, t.start_time
FROM v$session s
JOIN v$transaction t ON s.saddr = t.ses_addr;
-- ALTER SYSTEM KILL SESSION 'SID,SERIAL' IMMEDIATE;
ALTER SYSTEM KILL SESSION '468,27859' IMMEDIATE;
SELECT * FROM v$locked_object;
