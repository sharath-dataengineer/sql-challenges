--Problem 2: Sessionize Events with Dynamic Gaps

--Description:

--Given event logs, identify sessions where a new session starts if there's a 30-minute inactivity gap.


--Input data:

WITH events AS (
  SELECT * FROM (
    SELECT 1 AS user_id, TIMESTAMP '2024-07-01 09:00:00' AS event_time UNION ALL
    SELECT 1, TIMESTAMP '2024-07-01 09:10:00' UNION ALL
    SELECT 1, TIMESTAMP '2024-07-01 09:45:00' UNION ALL
    SELECT 1, TIMESTAMP '2024-07-01 10:50:00' UNION ALL
    SELECT 2, TIMESTAMP '2024-07-01 11:00:00' UNION ALL
    SELECT 2, TIMESTAMP '2024-07-01 11:31:00'
  ) AS t(user_id, event_time)
)

--Query:


WITH event_with_lag AS (
  SELECT *,
         LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time
  FROM events
),

with_flags AS (
  SELECT *,
         CASE
           WHEN prev_time IS NULL THEN 1
           WHEN UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(prev_time) > 1800 THEN 1
           ELSE 0
         END AS new_session_flag
  FROM event_with_lag
),

with_session_ids AS (
  SELECT *,
         SUM(new_session_flag) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
  FROM with_flags
)

SELECT user_id, session_id, MIN(event_time) AS session_start, MAX(event_time) AS session_end
FROM with_session_ids
GROUP BY user_id, session_id
ORDER BY user_id, session_id;


--Output:

-- | user_id  | session_id  | session_start       | session_end         |
-- | -------- | ----------- | ------------------- | ------------------- |
-- | 1        | 1           | 2024-07-01 09:00:00 | 2024-07-01 09:10:00 |
-- | 1        | 2           | 2024-07-01 09:45:00 | 2024-07-01 09:45:00 |
-- | 1        | 3           | 2024-07-01 10:50:00 | 2024-07-01 10:50:00 |
-- | 2        | 1           | 2024-07-01 11:00:00 | 2024-07-01 11:00:00 |
-- | 2        | 2           | 2024-07-01 11:31:00 | 2024-07-01 11:31:00 |

