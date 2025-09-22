-- *** Configuration tables ***

-- MV definitions
CREATE TABLE `mv/statements` (
   statement_no Int32 NOT NULL,
   statement_text Text NOT NULL,
   PRIMARY KEY(statement_no)
);

-- Externally controlled job definitions
CREATE TABLE `mv/jobs` (
    job_name Text NOT NULL,
    job_settings JsonDocument,
    should_run Bool,
    PRIMARY KEY(job_name)
);

-- Externally controlled scan requests
CREATE TABLE `mv/job_scans` (
    job_name Text NOT NULL,
    target_name Text NOT NULL,
    scan_settings JsonDocument,
    requested_at Timestamp,
    accepted_at Timestamp,
    runner_id Text,
    command_no Uint64,
    PRIMARY KEY(job_name, target_name)
);

-- *** Working tables ***

-- Dictionary changelog table
CREATE TABLE `mv/dict_hist` (
   src Text NOT NULL,
   tv Timestamp NOT NULL,
   seqno Uint64 NOT NULL,
   key_text Text NOT NULL,
   key_val JsonDocument,
   diff_val JsonDocument,
   PRIMARY KEY(src, tv, seqno, key_text)
);

-- Scans state table
CREATE TABLE `mv/scans_state` (
   job_name Text NOT NULL,
   table_name Text NOT NULL,
   updated_at Timestamp,
   key_position JsonDocument,
   PRIMARY KEY(job_name, table_name)
);

-- Runner instances status
CREATE TABLE `mv/runners` (
    runner_id Text NOT NULL,
    runner_identity Text,
    updated_at Timestamp,
    PRIMARY KEY(runner_id)
);

-- Jobs, per runner instance
CREATE TABLE `mv/runner_jobs` (
    runner_id Text NOT NULL,
    job_name Text NOT NULL,
    job_settings JsonDocument,
    started_at Timestamp,
    INDEX ix_job_name GLOBAL SYNC ON (job_name),
    PRIMARY KEY(runner_id, job_name)
);

-- Command control table via controller and runners
CREATE TABLE `mv/commands` (
    runner_id Text NOT NULL,
    command_no Uint64 NOT NULL,
    created_at Timestamp,
    command_type Text, -- START / STOP / SCAN / NOSCAN
    job_name Text,
    target_name Text,
    job_settings JsonDocument,
    command_status Text, -- CREATED / TAKEN / SUCCESS / ERROR
    command_diag Text,
    INDEX ix_command_no GLOBAL SYNC ON (command_no),
    PRIMARY KEY(runner_id, command_no)
);
