INSERT INTO STV2025032024__DWH.h_dialogs(hk_message_id, message_id,message_ts,load_dt,load_src)
select
       hash(message_id) as  hk_message_id,
       message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2025032024__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2025032024__DWH.h_dialogs);


INSERT INTO STV2025032024__DWH.h_groups(hk_user_id, group_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2025032024__STAGING.groups
where hash(id) not in (select hk_group_id from STV2025032024__DWH.h_groups);