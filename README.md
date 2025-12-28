# YDB materialized view processor

The YDB Materializer is a Java application that ensures data population for user-managed materialized views in YDB.

Each "materialized view" (MV) technically is a regular YDB table, which is updated using this application. The source information for populating the MV is retrieved from a set of other tables, which are linked together through SQL-style JOINs. To support online synchronization of changes from the source tables into the MV, YDB Change Data Capture streams are used.

The destination tables for MVs, source tables, required indexes and CDC streams should be created prior to using the application in MV synchronization mode. The application may help to generate DDL parts for some objects — for example, it reports missing indexes and can generate the proposed structure of destination tables.

[See the Releases page for downloads](https://github.com/ydb-platform/ydb-materializer/releases).

## Requirements and building

- Java 17 or higher
- YDB cluster 24.4+ with appropriate permissions
- Network access to YDB cluster
- Required system tables created in the database
- [Maven](https://maven.apache.org/) for building from source code

Building:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home
mvn clean package -DskipTests=true
```

## Usage

YDB Materializer can be embedded as a library in a user application, or used as a standalone application.

The description of materialized views and their processing jobs must be prepared using a special SQL-like language. The corresponding descriptions can be provided as a text file or as a database table. Connection settings and various technical parameters are provided as a set of properties (programmatically or as a Java Properties configuration file).

In standalone application mode, YDB Materializer implements:
- validation of materialized view and job definitions, including their compliance with the structure of source database tables, and output of corresponding error and warning messages for user analysis;
- generation and output of various SQL statements used by YDB Materializer, for further user analysis;
- service mode, in which it synchronizes the changes from source tables into the materialized view tables.

In embedded library mode, YDB Materializer implements all the listed functions, providing the ability to call them programmatically through methods of the corresponding classes.

## Materialized View Language Syntax

The YDB Materializer uses a custom SQL-like language to define materialized views and their processing handlers. This language is based on a subset of SQL with specific extensions for YDB materialized views.

### Language Overview

The language supports two main statement types:
1. **Materialized View Definition** - Defines the structure and logic of a materialized view
2. **Handler Definition** - Defines how to process change streams to update the materialized view

### Materialized View Definition

Basic materialized view:

```sql
CREATE ASYNC MATERIALIZED VIEW <view_name> AS
  SELECT <column_definitions>
  FROM <main_table> AS <alias>
  [<join_clauses>]
  [WHERE <filter_expression>];
```

Composite materialized view:

```sql
CREATE ASYNC MATERIALIZED VIEW <view_name> AS
  (SELECT <column_definitions>
   FROM <main_table1> AS <alias>
   [<join_clauses>]
   [WHERE <filter_expression>]) AS <select_alias1>
UNION ALL
  (SELECT <column_definitions>
   FROM <main_table2> AS <alias>
   [<join_clauses>]
   [WHERE <filter_expression>]) AS <select_alias2>
UNION ALL
   ...
   ;
```

A composite materialized view definition consists of two or more subqueries, each with the same syntax as a basic materialized view query, combined using the `UNION ALL` operator. Each subquery must also contain an alias that is unique within the composite materialized view and used to identify the subquery during its processing.

#### Column Definitions

Each column in the SELECT clause can be:
- **Direct column reference**: `table_alias.column_name AS column_alias`
- **Computed expression**: `#[<yql_expression>]# AS column_alias`
- **Computed expression with column references**: `COMPUTE ON table_alias.column_name, ... #[<yql_expression>]# AS column_alias`

#### Join Clauses

```sql
[INNER | LEFT] JOIN <table_name> AS <alias>
  ON <join_condition> [AND <join_condition>]*
```

Join conditions support:
- Column equality: `table1.col1 = table2.col2`
- Constant equality: `table1.col1 = 'value'` or `table1.col1 = 123`

#### Filter Expressions

The WHERE clause supports opaque (to the application) YQL expressions that are substituted unchanged directly into the generated queries:
```sql
WHERE COMPUTE ON table_alias.column_name, ... #[<yql_expression>]#
```

The presence of references to specific table and column names allows correct generation of derived SQL statements using opaque expressions that rely on specific columns of source tables.

### Handler Definition

```sql
CREATE ASYNC HANDLER <handler_name>
  [CONSUMER <consumer_name>]
  PROCESS <materialized_view_name>,
  [PROCESS <materialized_view_name>,]
  INPUT <table_name> CHANGEFEED <changefeed_name> AS [STREAM|BATCH],
  [INPUT <table_name> CHANGEFEED <changefeed_name> AS [STREAM|BATCH], ...];
```

#### Handler Components

- **PROCESS**: Specifies which materialized views this handler updates
- **INPUT**: Defines input tables and their changefeed streams
  - `STREAM`: Real-time processing of individual changes
  - `BATCH`: Batch processing of accumulated changes
- **CONSUMER**: Optional consumer name for the changefeed

### Opaque Expressions

The language supports opaque expressions wrapped in `#[` and `]#` delimiters. These contain YQL (Yandex Query Language) code that is passed through to the database without parsing:

```sql
-- In SELECT clause
SELECT #[Substring(main.c20, 3, 5)]# AS c11,
       #[CAST(NULL AS Int32?)]# AS c12

-- In WHERE clause
WHERE #[main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u)]#

-- With COMPUTE ON clause
COMPUTE ON main, sub2 #[main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u)]#
```

### Complete Example

```sql
-- Define a materialized view
CREATE ASYNC MATERIALIZED VIEW `test1/mv1` AS
  SELECT main.id AS id,
         main.c1 AS c1,
         main.c2 AS c2,
         main.c3 AS c3,
         sub1.c8 AS c8,
         sub2.c9 AS c9,
         sub3.c10 AS c10,
         #[Substring(main.c20, 3, 5)]# AS c11,
         #[CAST(NULL AS Int32?)]# AS c12
  FROM `test1/main_table` AS main
  INNER JOIN `test1/sub_table1` AS sub1
    ON main.c1 = sub1.c1 AND main.c2 = sub1.c2
  LEFT JOIN `test1/sub_table2` AS sub2
    ON main.c3 = sub2.c3 AND 'val1'u = sub2.c4
  INNER JOIN `test1/sub_table3` AS sub3
    ON sub3.c5 = 58
  WHERE #[main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u)]#;

-- Define a handler to process changes
CREATE ASYNC HANDLER h1 CONSUMER h1_consumer
  PROCESS `test1/mv1`,
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  INPUT `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  INPUT `test1/sub_table3` CHANGEFEED cf4 AS BATCH;
```

### Language Features

- **Case-insensitive keywords**: All SQL keywords are case-insensitive
- **Quoted identifiers**: Use backticks for identifiers with special characters: `` `table/name` ``
- **String literals**: Single-quoted strings with optional type suffixes (`'value'u` for Utf8)
- **Comments**: Both `--` line comments and `/* */` block comments
- **Semicolon termination**: Statements must be terminated with semicolons

### Supported Data Types

The language works with standard YDB data types:
- **Text**: String data (use `'value'u` for Utf8 strings)
- **Numeric**: Int32, Int64, Decimal, etc.
- **Temporal**: Timestamp, Date, etc.
- **Complex**: JsonDocument, etc.

## Command Line Syntax

```bash
java -jar ydb-materializer-*.jar <config.xml> <MODE>
```

The application supports three operational modes:
- CHECK: configuration validation;
- SQL: generating SQL statements representing the materialization logic;
- RUN: actual MV synchronization.

**Parameters:**
- `<config.xml>` - Path to the XML configuration file
- `<MODE>` - Operation mode: `CHECK`, `SQL`, or `RUN`

### Operation Modes

#### CHECK Mode
Validates materialized view definitions and reports any issues:
```bash
java -jar ydb-materializer-*.jar config.xml CHECK
```

#### SQL Mode
Generates and outputs the SQL statements for materialized views:
```bash
java -jar ydb-materializer-*.jar config.xml SQL
```

#### LOCAL Mode
Starts a local, single-node materialized view processing service:
```bash
java -jar ydb-materializer-*.jar config.xml LOCAL
```

#### JOB Mode
Starts a distributed materialized view processing service:
```bash
java -jar ydb-materializer-*.jar config.xml JOB
```

## Configuration File

The configuration file is an XML properties file that defines connection parameters and job settings. Here's an example configuration:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
<comment>YDB Materializer sample configuration</comment>

<!-- *** Connection parameters *** -->
<entry key="ydb.url">grpcs://ydb01.localdomain:2135/cluster1/testdb</entry>
<entry key="ydb.cafile">/path/to/ca.crt</entry>
<entry key="ydb.poolSize">1000</entry>
<entry key="ydb.preferLocalDc">false</entry>

<!-- Authentication mode: NONE, ENV, STATIC, METADATA, SAKEY -->
<entry key="ydb.auth.mode">STATIC</entry>
<entry key="ydb.auth.username">root</entry>
<entry key="ydb.auth.password">your_password</entry>

<!-- Input mode: FILE or TABLE -->
<entry key="job.input.mode">FILE</entry>
<entry key="job.input.file">example-job1.sql</entry>
<entry key="job.input.table">mv/statements</entry>

<!-- Handler configuration -->
<entry key="job.handlers">h1,h2,h3</entry>
<entry key="job.scan.rate">10000</entry>
<entry key="job.scan.table">mv/scans_state</entry>
<entry key="job.coordination.path">mv/coordination</entry>
<entry key="job.coordination.timeout">10</entry>

<!-- Dictionary scanner configuration -->
<entry key="job.dict.consumer">dictionary</entry>
<entry key="job.dict.hist.table">mv/dict_hist</entry>
<entry key="job.dict.scan.seconds">28800</entry>

<!-- Performance tuning -->
<entry key="job.apply.partitioning">HASH</entry>
<entry key="job.cdc.threads">4</entry>
<entry key="job.apply.threads">4</entry>
<entry key="job.apply.queue">10000</entry>
<entry key="job.batch.select">1000</entry>
<entry key="job.batch.upsert">500</entry>
<entry key="job.max.row.changes">100000</entry>
<entry key="job.query.seconds">30</entry>

<!-- Management settings -->
<entry key="mv.jobs.table">mv_jobs</entry>
<entry key="mv.scans.table">mv_job_scans</entry>
<entry key="mv.runners.table">mv_runners</entry>
<entry key="mv.runner.jobs.table">mv_runner_jobs</entry>
<entry key="mv.commands.table">mv_commands</entry>
<entry key="mv.scan.period.ms">5000</entry>
<entry key="mv.report.period.ms">10000</entry>
<entry key="mv.runner.timeout.ms">30000</entry>
<entry key="mv.coord.startup.ms">90000</entry>
<entry key="mv.coord.runners.count">1</entry>

</properties>
```

### Configuration Parameters Reference

#### Database Connection
- `ydb.url` - YDB connection string (required)
- `ydb.cafile` - Path to TLS certificate file (optional)
- `ydb.poolSize` - Connection pool size (default: 2 × CPU cores)
- `ydb.preferLocalDc` - Prefer local data center (default: false)

#### Authentication
- `ydb.auth.mode` - Authentication mode:
  - `NONE` - No authentication
  - `ENV` - Environment variables
  - `STATIC` - Username/password
  - `METADATA` - VM metadata
  - `SAKEY` - Service account key file
- `ydb.auth.username` - Username (for STATIC mode)
- `ydb.auth.password` - Password (for STATIC mode)
- `ydb.auth.sakey` - Path to service account key file (for SAKEY mode)

#### Job Configuration
- `job.input.mode` - Input source: `FILE` or `TABLE`
- `job.input.file` - Path to SQL file (for FILE mode)
- `job.input.table` - Table name for statements (for TABLE mode)
- `job.handlers` - Comma-separated list of handler names to activate
- `job.scan.table` - Scan position control table name
- `job.dict.hist.table` - Dictionary history table name
- `job.coordination.path` - Coordination service node path
- `job.coordination.timeout` - Lock timeout for job coordination in seconds

#### Dictionary scanner configuration
- `job.dict.consumer` - consumer name to be used for dictionary table changefeeds
- `job.dict.hist.table` - alternative name for MV_DICT_HIST table
- `job.dict.scan.seconds` - period between the dictionary changes checks

#### Performance Tuning
- `job.apply.partitioning` - HASH (default) or RANGE partitioning of apply tasks
- `job.cdc.threads` - Number of CDC reader threads
- `job.apply.threads` - Number of apply worker threads
- `job.apply.queue` - Max elements in apply queue per thread
- `job.batch.select` - Batch size for SELECT operations
- `job.batch.upsert` - Batch size for UPSERT or DELETE operations
- `job.max.row.changes` - Maximum number of changes per individual table processed in one iteration
- `job.query.seconds` — Maximum query execution time for SELECT, UPSERT or DELETE operations, seconds

#### Management Settings
- `mv.jobs.table` - Custom MV_JOBS table name
- `mv.scans.table` - Custom MV_JOB_SCANS table name
- `mv.runners.table` - Custom MV_RUNNERS table name
- `mv.runner.jobs.table` - Custom MV_RUNNER_JOBS table name
- `mv.commands.table` - Custom MV_COMMANDS table name
- `mv.scan.period.ms` - Runner and Coordinator re-scan period, in milliseconds
- `mv.report.period.ms` - Runner status report period, in milliseconds
- `mv.runner.timeout.ms` - Runner and Coordinator missing timeout period, in milliseconds
- `mv.coord.startup.ms` - The delay between the Coordinator startup and job distribution activation, milliseconds
- `mv.coord.runners.count` - The minimal number of Runners for job distribution

## Distributed Job Management (JOB Mode)

The JOB mode provides distributed job management capabilities, allowing to manage materialized view processing tasks across multiple instances. This mode is invoked using:

```bash
java -jar ydb-materializer.jar config.xml JOB
```

### Architecture Overview

The distributed job management system consists of two main components:

1. **MvRunner** - Executes jobs locally on each instance
2. **MvCoordinator** - Manages job distribution and coordination across runners

Each job is a running instance of the "handler" defined in the configuration.

On job startup, the configuration is re-read and re-validated by the application, and used in the particular job.

### Control Tables

The system uses several YDB tables to manage distributed operations:

#### Configuration Tables

**`mv_jobs`** - Job definitions and desired state
```sql
CREATE TABLE `mv_jobs` (
    job_name Text NOT NULL,           -- Handler name
    job_settings JsonDocument,        -- Handler configuration
    should_run Bool,                  -- Whether job should be running
    PRIMARY KEY(job_name)
);
```

**`mv_job_scans`** - Scan requests for specific targets
```sql
CREATE TABLE `mv_job_scans` (
    job_name Text NOT NULL,           -- Handler name
    target_name Text NOT NULL,        -- Target table name
    scan_settings JsonDocument,       -- Scan configuration
    requested_at Timestamp,           -- When scan was requested
    accepted_at Timestamp,            -- When scan was accepted
    runner_id Text,                   -- Assigned runner ID
    command_no Uint64,                -- Command number
    PRIMARY KEY(job_name, target_name)
);
```

#### Working Tables

**`mv_runners`** - Active runner instances
```sql
CREATE TABLE `mv_runners` (
    runner_id Text NOT NULL,          -- Unique runner identifier
    runner_identity Text,             -- Host, PID, start time info
    updated_at Timestamp,             -- Last status update
    PRIMARY KEY(runner_id)
);
```

**`mv_runner_jobs`** - Currently running jobs per runner
```sql
CREATE TABLE `mv_runner_jobs` (
    runner_id Text NOT NULL,          -- Runner identifier
    job_name Text NOT NULL,           -- Job name
    job_settings JsonDocument,        -- Job configuration
    started_at Timestamp,             -- When job started
    INDEX ix_job_name GLOBAL SYNC ON (job_name),
    PRIMARY KEY(runner_id, job_name)
);
```

**`mv_commands`** - Command queue for runners
```sql
CREATE TABLE `mv_commands` (
    runner_id Text NOT NULL,          -- Target runner
    command_no Uint64 NOT NULL,       -- Command sequence number
    created_at Timestamp,             -- Command creation time
    command_type Text,                -- START/STOP/SCAN/NOSCAN
    job_name Text,                    -- Target job name
    target_name Text,                 -- Target table (for scans)
    job_settings JsonDocument,        -- Job configuration
    command_status Text,              -- CREATED/TAKEN/SUCCESS/ERROR
    command_diag Text,                -- Error diagnostics
    INDEX ix_command_no GLOBAL SYNC ON (command_no),
    INDEX ix_command_status GLOBAL SYNC ON (command_status, runner_id),
    PRIMARY KEY(runner_id, command_no)
);
```

### Job Management Operations

#### Adding Jobs

To add a new job, insert a record into the `mv_jobs` table:

```sql
INSERT INTO `mv_jobs` (job_name, job_settings, should_run)
VALUES ('my_handler', NULL, true);
```

The coordinator will automatically detect the new job and assign it to an available runner.

The `job_settings` can be omitted (so that the default parameters will be used, loaded from global settings) or specified as a JSON document of the following format:

```json
{ # comment indicates the corresponding global setting
    "cdcReaderThreads": 4,                # job.cdc.threads
    "applyThreads": 4,                    # job.apply.threads
    "applyQueueSize": 10000,              # job.apply.queue
    "selectBatchSize": 1000,              # job.batch.select
    "upsertBatchSize": 500,               # job.batch.upsert
    "dictionaryScanSeconds": 28800        # job.dict.scan.seconds
}
```

The example above shows the defaults for regular jobs. For special dictionary scanner job the following settings can be specified:

```json
{
    "upsertBatchSize": 500,               # job.batch.upsert
    "cdcReaderThreads": 4,                # job.cdc.threads
    "rowsPerSecondLimit": 10000,          # job.scan.rate
    "maxChangeRowsScanned": 100000        # job.max.row.changes
}
```

#### Removing Jobs

To stop and remove a job:

```sql
UPDATE `mv_jobs` SET should_run = false WHERE job_name = 'my_handler';
-- or
DELETE FROM `mv_jobs` WHERE job_name = 'my_handler';
```

#### Requesting Scans

To request a scan of a specific target table:

```sql
INSERT INTO `mv_job_scans` (job_name, target_name, scan_settings, requested_at)
VALUES ('my_handler', 'target_table', '{"rowsPerSecondLimit": 5000}', CurrentUtcTimestamp());
```

#### Monitoring Operations

**Check running jobs:**
```sql
SELECT rj.runner_id, rj.job_name, rj.started_at, r.runner_identity
FROM `mv_runner_jobs` rj
JOIN `mv_runners` r ON rj.runner_id = r.runner_id;
```

**Check job status:**
```sql
SELECT j.job_name, j.should_run,
       CASE WHEN rj.job_name IS NOT NULL THEN 'RUNNING'u ELSE 'STOPPED'u END as status
FROM `mv_jobs` j
LEFT JOIN `mv_runner_jobs` rj ON j.job_name = rj.job_name;
```

**Check command queue:**
```sql
SELECT runner_id, command_no, command_type, job_name, command_status, created_at
FROM `mv_commands`
WHERE command_status IN ('CREATED'u, 'TAKEN'u)
ORDER BY created_at;
```

**Check runner status:**
```sql
SELECT runner_id, runner_identity, updated_at
FROM `mv_runners`
ORDER BY updated_at DESC;
```

### Command Types

The system supports four types of commands:

- **START** - Start a job on a runner
- **STOP** - Stop a job on a runner
- **SCAN** - Start scanning a specific target table
- **NOSCAN** - Stop the already running scan for a specific target table

### Job Names

Job name for regular jobs refer to the handler name. There are two special job names:

- **`ydbmv$dictionary`** - Dictionary scanner (manually managed)
- **`ydbmv$coordinator`** - Coordinator job (automatically managed)

These special names cannot be used for regular handlers (in fact handler name cannot start with prefix "ydbmv").

### Fault Tolerance

The system provides automatic fault tolerance:

1. **Runner Failure Detection** - Runners report status periodically; inactive runners are automatically cleaned up
2. **Job Rebalancing** - When runners fail, their jobs are automatically reassigned to available runners
3. **Command Retry** - Failed commands remain in the queue for retry
4. **Leader Election** - Only one coordinator instance is active at a time

### Deployment

1. **Create Control Tables** - Use the provided `example-tables.sql` script. Table names can be customized as needed
2. **Deploy Runners** - Start multiple instances with `JOB` mode
3. **Configure Jobs** - Insert job definitions into `mv_jobs` table. Add the scan definitions to the `mv_job_scans` table
4. **Monitor Operations** - Use the monitoring queries listed above

The system will automatically distribute jobs across available runners and maintain the desired state.
