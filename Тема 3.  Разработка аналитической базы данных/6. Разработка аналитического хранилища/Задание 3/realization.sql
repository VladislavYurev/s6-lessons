DROP TABLE IF EXISTS STV2025032024__DWH.l_user_message;

CREATE TABLE STV2025032024__DWH.l_user_message (
  hk_l_user_message BIGINT PRIMARY KEY,
  hk_user_id BIGINT NOT NULL REFERENCES STV2025032024__DWH.h_users (hk_user_id),
  hk_message_id BIGINT NOT NULL REFERENCES STV2025032024__DWH.h_dialogs (hk_message_id),
  load_dt TIMESTAMP,
  load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);




DROP TABLE IF EXISTS STV2025032024__DWH.l_groups_dialogs;

CREATE TABLE STV2025032024__DWH.l_groups_dialogs (
  hk_l_groups_dialogs BIGINT PRIMARY KEY,
  hk_message_id BIGINT NOT NULL REFERENCES STV2025032024__DWH.h_dialogs (hk_message_id),
  hk_group_id BIGINT NOT NULL REFERENCES STV2025032024__DWH.h_groups (hk_group_id),
  load_dt TIMESTAMP,
  load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_groups_dialogs ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


DROP TABLE IF EXISTS STV2025032024__DWH.l_admins;

CREATE TABLE STV2025032024__DWH.l_admins (
  hk_l_admin_id BIGINT PRIMARY KEY,
  hk_user_id BIGINT NOT NULL REFERENCES STV2025032024__DWH.h_users (hk_user_id),
  hk_group_id BIGINT NOT NULL REFERENCES STV2025032024__DWH.h_groups (hk_group_id),
  load_dt TIMESTAMP,
  load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_admin_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

