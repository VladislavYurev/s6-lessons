(SELECT 
    min(u.registration_dt) as datestamp,
    'earliest user registration' as info
FROM STV2025032024__STAGING.users u)
UNION ALL
(SELECT
    max(u.registration_dt) as datestamp,
    'latest user registration'
FROM STV2025032024__STAGING.users u)
UNION ALL
(SELECT 
    min(g.registration_dt) as datestamp,
    'earliest user registration' as info
FROM STV2025032024__STAGING.groups g)
UNION ALL
(SELECT
    max(g.registration_dt) as datestamp,
    'latest user registration'
FROM STV2025032024__STAGING.groups g)
UNION ALL
(SELECT 
    min(d.message_ts) as datestamp,
    'earliest user registration' as info
FROM STV2025032024__STAGING.dialogs d)
UNION ALL
(SELECT
    max(d.message_ts) as datestamp,
    'latest user registration'
FROM STV2025032024__STAGING.dialogs d);