--Input data:

WITH user_activity AS (
  SELECT * FROM (
    SELECT 101 AS user_id, DATE '2024-01-01' AS event_date UNION ALL
    SELECT 101, DATE '2024-01-05' UNION ALL
    SELECT 101, DATE '2024-03-10' UNION ALL
    SELECT 102, DATE '2024-01-15' UNION ALL
    SELECT 102, DATE '2024-02-20' UNION ALL
    SELECT 102, DATE '2024-03-25' UNION ALL
    SELECT 103, DATE '2024-04-01'
  ) AS t(user_id, event_date)
)

--Query:

WITH user_activity_with_lag AS (
  SELECT *,
         LAG(event_date) OVER (PARTITION BY user_id ORDER BY event_date) AS previous_date
  FROM user_activity
),

gaps AS (
  SELECT *,
         DATEDIFF(event_date, previous_date) AS gap_days
  FROM user_activity_with_lag
)
SELECT DISTINCT user_id
FROM gaps
WHERE gap_days > 30;


--Output:

--| user_id  |
--| -------- |
--| 101      |
--| 102      |

