# YDB materialized view processor

The YDB Materializer is a Java application that processes user-managed materialized views in YDB.

[See the Releases page for downloads](https://github.com/zinal/ydb-materializer/releases).

## Usage

Each "materialized view" (MV) technically is a regular YDB table, which is updated using this application. The source information is retrieved from the set of the original tables, which are linked together through the SQL-style JOINs. To support the online sync of the source tables' changes into the MV, YDB Change Data Capture streams are used.

The destination tables for MVs, source tables, required indexes and CDC streams should be created prior to using the application for synchronization. The application may help to generate the DDL parts of some of the objects - for example it reports the missing indexes, and can generate the proposed structure of the destination tables.

## Materialized View Language Syntax

The YDB Materializer uses a custom SQL-like language to define materialized views and their processing handlers. This language is based on a subset of SQL with specific extensions for YDB materialized views.

### Language Overview

The language supports two main statement types:
1. **Materialized View Definition** - Defines the structure and logic of a materialized view
2. **Handler Definition** - Defines how to process change streams to update the materialized view

### Materialized View Definition

```sql
CREATE ASYNC MATERIALIZED VIEW <view_name> AS
  SELECT <column_definitions>
  FROM <main_table> AS <alias>
  [<join_clauses>]
  [WHERE <filter_expression>];
```

#### Column Definitions

Each column in the SELECT clause can be:
- **Direct column reference**: `table_alias.column_name AS column_alias`
- **Computed expression**: `#[<yql_expression>]# AS column_alias`

#### Join Clauses

```sql
[INNER | LEFT] JOIN <table_name> AS <alias>
  ON <join_condition> [AND <join_condition>]*
```

Join conditions support:
- Column equality: `table1.col1 = table2.col2`
- Constant equality: `table1.col1 = 'value'` or `table1.col1 = 123`

#### Filter Expressions

The WHERE clause supports opaque YQL expressions:
```sql
WHERE #[<yql_expression>]#
```

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

#### RUN Mode
Starts the materialized view processing service:
```bash
java -jar ydb-materializer-*.jar config.xml RUN
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

<!-- Handler configuration -->
<entry key="job.handlers">h1</entry>
<entry key="job.scan.table">mv/scans_state</entry>
<entry key="job.coordination.path">mv/coordination</entry>

<!-- Performance tuning -->
<entry key="job.default.cdc.threads">4</entry>
<entry key="job.default.apply.threads">4</entry>
<entry key="job.default.apply.queue">10000</entry>
<entry key="job.default.batch.select">1000</entry>
<entry key="job.default.batch.upsert">500</entry>
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
- `job.scan.table` - Scan position control table
- `job.coordination.path` - Coordination service node path

#### Performance Tuning
- `job.default.cdc.threads` - Number of CDC reader threads
- `job.default.apply.threads` - Number of apply worker threads
- `job.default.apply.queue` - Max elements in apply queue per thread
- `job.default.batch.select` - Batch size for SELECT operations
- `job.default.batch.upsert` - Batch size for UPSERT operations

## Requirements

- Java 21 or higher
- YDB cluster with appropriate permissions
- Network access to YDB cluster
- Required system tables created in the database

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home
mvn clean package -DskipTests=true
```
