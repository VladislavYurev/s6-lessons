drop table if exists STV2025032024__STAGING.users;

create table STV2025032024__STAGING.users (
	id int not null,
	chat_name varchar(200),
	registration_dt timestamp,
	country varchar(200),
	age int
)
order by id
SEGMENTED BY HASH(id) ALL NODES;



drop table if exists STV2025032024__STAGING.dialogs;

create table STV2025032024__STAGING.dialogs (
	message_id int not null,
	message_ts timestamp,
	message_from int,
	message_to int,
	message varchar(1000),
	message_group int
)
order by message_id
SEGMENTED BY HASH(message_id) ALL NODES
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);

