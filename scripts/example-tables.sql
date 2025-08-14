-- An example of YDB table for MV definitions
CREATE TABLE `mv/statements` (
   statement_no Int32 NOT NULL,
   statement_text Text NOT NULL,
   PRIMARY KEY(statement_no);
);

-- db-scheduler table, in case the scheduler is used
CREATE TABLE `mv/scheduled_tasks` (
    task_name Text NOT NULL,
    task_instance Text NOT NULL,
    task_data String,
    execution_time Timestamp,
    picked Bool,
    picked_by Text,
    last_success Timestamp,
    last_failure Timestamp,
    consecutive_failures Int32,
    last_heartbeat Timestamp,
    version Int64,
    priority Int32,
    PRIMARY KEY (task_name, task_instance)
);

-- Tasks state table
CREATE TABLE `mv/tasks_state` (
   task_name Text NOT NULL,
   task_instance Text NOT NULL,
   task_state String,
   PRIMARY KEY(task_name, task_instance);
);


-- ************

-- An example schema for the source tables.
CREATE TABLE `test1/main_table` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c20 Text,
    PRIMARY KEY(id),
    INDEX ix_c1 GLOBAL ON (c1)
);

CREATE TABLE `test1/sub_table1` (
    c1 Timestamp,
    c2 Int64,
    c8 Int32,
    PRIMARY KEY(c1, c2)
);

CREATE TABLE `test1/sub_table2` (
    c3 Decimal(22,9),
    c4 Text,
    c9 Date,
    PRIMARY KEY(c3, c4)
);

CREATE TABLE `test1/sub_table3` (
    c5 Int32 NOT NULL,
    c10 Text,
    PRIMARY KEY(c5)
);

CREATE TABLE `test1/mv1` (
    id Text NOT NULL,
    c1 Timestamp,
    c2 Int64,
    c3 Decimal(22,9),
    c8 Int32,
    c9 Date,
    c10 Text,
    c11 Text,
    c12 Int32,
    PRIMARY KEY(id),
    INDEX ix_c1 GLOBAL ON (c1)
);

ALTER TABLE `test1/main_table` ADD CHANGEFEED `cf1` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table1` ADD CHANGEFEED `cf2` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table2` ADD CHANGEFEED `cf3` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
ALTER TABLE `test1/sub_table3` ADD CHANGEFEED `cf4` WITH (FORMAT = 'JSON', MODE = 'KEYS_ONLY');
