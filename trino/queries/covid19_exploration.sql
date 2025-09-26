-- List tables registered in the default schema
SHOW TABLES;

-- Inspect the schema for the covid19 table
DESCRIBE covid19;

-- Daily progression for a specific country
SELECT country,
       reportdate,
       confirmed
FROM covid19
WHERE country = 'Italy'
  AND reportdate BETWEEN DATE '2020-03-01' AND DATE '2020-03-14'
ORDER BY reportdate;

-- Countries with the highest confirmed counts
SELECT country,
       MAX(confirmed) AS peak_confirmed
FROM covid19
GROUP BY country
ORDER BY peak_confirmed DESC
LIMIT 10;
