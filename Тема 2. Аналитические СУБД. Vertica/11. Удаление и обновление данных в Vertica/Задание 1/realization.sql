SET SESSION AUTOCOMMIT TO off;

DELETE FROM STV2025032024.members WHERE age > 45;

SELECT node_name, projection_name, deleted_row_count FROM DELETE_VECTORS
	where projection_name like 'members%';

SELECT max(deleted_row_count) FROM DELETE_VECTORS
	where projection_name like 'members%';

ROLLBACK;