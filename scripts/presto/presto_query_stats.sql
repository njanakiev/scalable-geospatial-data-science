SELECT 
  substring(regexp_split(query, '\n')[1], 1, 20) AS q, 
  date_diff('millisecond', created, "end")/1000.0 AS total
FROM system.runtime.queries 
WHERE state = 'FINISHED' 
  AND source = 'presto-cli'
ORDER BY "end";
