select
	'users' as dataset,
    count(id) as total,
    count(distinct id) as uniq
from STV2025032024__STAGING.users
UNION ALL
select
	'groups' as dataset,
    count(id) as total,
    count(distinct id) as uniq
from STV2025032024__STAGING.groups
UNION ALL
select
	'dialogs' as dataset,
    count(message_id) as total,
    count(distinct message_id) as uniq
from STV2025032024__STAGING.dialogs;    