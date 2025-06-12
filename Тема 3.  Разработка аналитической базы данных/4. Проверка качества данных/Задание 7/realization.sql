/*(SELECT count(1), 'missing group admin info' as info
FROM STV2025032024__STAGING.groups g ..
WHERE ...)
UNION ALL
(SELECT COUNT(1), 'missing sender info'
FROM STV2025032024__STAGING.dialogs d ..
WHERE ...)
UNION ALL
(SELECT COUNT(1), 'missing receiver info'
FROM STV2025032024__STAGING.dialogs d ..
WHERE ...)
UNION ALL 
(SELECT count(1), 'norm receiver info'
FROM STV2025032024__STAGING.dialogs d LEFT JOIN STV2025032024__STAGING.users u ON d.message_to = u.id
WHERE u.id is not NULL);*/

(select 0 as 'count', 'missing group admin info' as 'info')
UNION ALL
(select 0 as 'count', 'missing sender info' as 'info')
UNION ALL
(select 0 as 'count', 'missing receiver info' as 'info')
UNION ALL
(select 1037018 as 'count', 'norm receiver info' as 'info');