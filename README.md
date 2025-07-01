SELECT DISTINCT
    sent_month,
    id_account,
    sent_count,
    sent_count_month,
    sent_count / sent_count_month * 100 AS sent_msg_percent,
    first_sent_date,
    last_sent_date
FROM (
    SELECT
        DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
        es.id_account,
        COUNT(es.id_message) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS sent_count,
        COUNT(es.id_message) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS sent_count_month,
        MIN(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS first_sent_date,
        MAX(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS last_sent_date
    FROM `data-analytics-mate.DA.email_sent` es
    JOIN `data-analytics-mate.DA.account_session` ac ON es.id_account = ac.account_id
    JOIN `data-analytics-mate.DA.session` s ON ac.ga_session_id = s.ga_session_id
) AS functions
