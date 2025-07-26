--Problem 1: Get the Latest Status Update per Ticket

--Description:

--You are tasked with identifying the latest status update for each ticket in a helpdesk system.

--Input data:

WITH ticket_updates AS (
  SELECT * FROM (
    SELECT 101 AS ticket_id, 'open' AS status, TIMESTAMP '2024-06-01 10:00:00' AS updated_at UNION ALL
    SELECT 101, NULL, TIMESTAMP '2024-06-02 10:00:00' UNION ALL
    SELECT 101, 'closed', TIMESTAMP '2024-06-03 10:00:00' UNION ALL
    SELECT 102, 'open', TIMESTAMP '2024-06-01 09:00:00' UNION ALL
    SELECT 102, NULL, TIMESTAMP '2024-06-04 12:00:00' UNION ALL
    SELECT 103, NULL, TIMESTAMP '2024-06-01 08:00:00'
  ) AS t(ticket_id, status, updated_at)
)


--Query:

SELECT ticket_id,
       MAX_BY(status, updated_at) AS latest_status
FROM ticket_updates
WHERE status IS NOT NULL
GROUP BY ticket_id;



--Output:

-- | ticket_id  | latest_status  |
-- | ---------- | -------------- |
-- | 101        | closed         |
-- | 102        | open           |
