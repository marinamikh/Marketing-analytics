WITH
  final AS (
  WITH
    session_ids AS (
    WITH
      events AS (
      SELECT
        user_pseudo_id,
        campaign,
        category,
        country,
        event_name,
        TIMESTAMP_MICROS(event_timestamp) AS event_occured,
        LAG(TIMESTAMP_MICROS(event_timestamp),1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp, event_name) AS prev_event
      FROM
        `turing_data_analytics.raw_events`
      ORDER BY
        1),
      duration AS (
      SELECT
        *,
        CASE
          WHEN DATE_DIFF(event_occured, prev_event, MINUTE) >= 30 OR prev_event IS NULL THEN 1
        ELSE
        0
      END
        AS is_new_session
      FROM
        events)
    SELECT
      DISTINCT user_pseudo_id,
      FORMAT_DATE('%A', event_occured) AS day_of_week,
      event_occured,
      prev_event,
      campaign,
      category,
      country,
      event_name,
      is_new_session,
      SUM(is_new_session) OVER (ORDER BY user_pseudo_id, event_occured) AS global_session_id,
      SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_occured) AS user_session_id
    FROM
      duration
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
    ORDER BY
      1,
      3)
  SELECT
    *,
    MAX(campaign) OVER (PARTITION BY event_occured, global_session_id) AS campaign_id,
    MIN(event_occured) OVER (PARTITION BY global_session_id) AS start_time,
    MAX(event_occured) OVER (PARTITION BY global_session_id) AS end_time,
    ROW_NUMBER () OVER (PARTITION BY user_pseudo_id, global_session_id ORDER BY event_occured) AS ranking
  FROM
    session_ids)
SELECT
  user_pseudo_id,
  day_of_week,
  campaign_id,
  category,
  country,
  event_name,
  start_time,
  end_time,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_sec,
  ROUND(TIMESTAMP_DIFF(end_time, start_time, SECOND)/60, 2) AS duration_min
FROM
  final
WHERE
  ranking=1
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
ORDER BY
  user_pseudo_id,
  start_time