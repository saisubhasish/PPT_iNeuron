use tier_task;


WITH session_data AS (
  SELECT
    anonymous_id,
    event_name,
    created_at,
    LAG(created_at) OVER (PARTITION BY anonymous_id ORDER BY created_at) AS prev_created_at,
    COALESCE(created_at - LAG(created_at) OVER (PARTITION BY anonymous_id ORDER BY created_at), INTERVAL '0') AS time_diff
  FROM
    mobile_events
  ORDER BY
    anonymous_id,
    created_at
),
sessions AS (
  SELECT
    anonymous_id,
    event_name,
    created_at,
    SUM(CASE WHEN time_diff > INTERVAL '30 minutes' OR prev_created_at IS NULL THEN 1 ELSE 0 END) OVER (ORDER BY anonymous_id, created_at) AS session_id
  FROM
    session_data
)
SELECT
  anonymous_id,
  MIN(created_at) AS session_start,
  MAX(created_at) AS session_end,
  COUNT(DISTINCT CASE WHEN event_name = 'book_scooter' THEN session_id END) AS sessions_with_booking_intention,
  COUNT(DISTINCT CASE WHEN event_name = 'booking_successful' THEN session_id END) AS sessions_with_successful_booking
FROM
  sessions
GROUP BY
  anonymous_id, session_id;


SELECT
  (sessions_with_booking_intention::decimal / COUNT(DISTINCT session_id)) * 100 AS percentage_of_sessions_with_booking_intention
FROM
  sessions;



SELECT
  (sessions_with_successful_booking::decimal / COUNT(DISTINCT session_id)) * 100 AS percentage_of_sessions_with_successful
