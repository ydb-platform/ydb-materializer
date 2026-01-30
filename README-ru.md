# Процессор материализованных представлений YDB

YDB Materializer — это Java-приложение, которое обеспечивает наполнение данными управляемых пользователем материализованных представлений в YDB.

Каждое «материализованное представление» (MV) технически представляет собой обычную таблицу YDB, которая обновляется с помощью этого приложения. Исходная информация для наполнения MV извлекается из набора других таблиц, связанных друг с другом с помощью JOIN в стиле SQL. Для поддержки онлайн-синхронизации изменений исходных таблиц в MV используются потоки YDB Change Data Capture.

Таблицы назначения для MV, исходные таблицы, необходимые индексы и CDC-потоки должны быть созданы до использования приложения в режиме синхронизации MV. Приложение может помочь сгенерировать части DDL для некоторых объектов — например, оно сообщает о недостающих индексах и может сгенерировать предлагаемую структуру таблиц назначения.

[Скачать приложение можно на странице релизов](https://github.com/ydb-platform/ydb-materializer/releases).

## Системные требования и порядок сборки

- Java 21 или выше.
- Кластер YDB 24.4+ с соответствующими разрешениями.
- Сетевой доступ к кластеру YDB.
- Необходимые системные таблицы, созданные в базе данных.
- Для сборки из исходных кодов - [Maven](https://maven.apache.org/)

Сборка:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home
mvn clean package -DskipTests=true
```

## Использование

YDB Materializer может быть встроен как библиотека в пользовательской приложение, либо применён как автономное приложение.

Описание материализованных представлений и заданий по их обработке необходимо подготовить с использованием специального SQL-подобного языка. Соответствующие описания могут быть поданы в виде текстового файла, либо в виде таблицы БД. Настройки подключения к БД и различные технические параметры подаются в виде набора свойств (программно либо в виде конфигурационного файла Java Properties).

В режиме автономного приложения YDB Materializer реализует:
- проверку корректности описаний материализованных представлений и заданий, включая соответствие их структуре исходных таблиц БД, и вывод для анализа пользователем соответствующих сообщений об ошибках и предупреждений;
- формирование и вывод для анализа пользователем различных SQL-операторов, используемых при работе средств материализации;
- работу в режиме сервиса, выполняющего синхронизацию изменений из исходных таблиц в таблицы материализованных представлений.

В режиме встраиваемой библиотеки YDB Materializer реализует все перечисленные функции, предоставляя возможность их программного вызова через методы соответствующих классов.

## Синтаксис языка материализованных представлений

YDB Materializer использует специальный SQL-подобный язык для определения материализованных представлений и заданий их обработки (обработчиков). Этот язык основан на подмножестве SQL с особыми расширениями для поддержки пользовательских материализованных представлений YDB.

### Обзор языка

Язык поддерживает два основных типа инструкций:
1. **Определение материализованного представления** — определяет структуру и логику материализованного представления.
2. **Определение обработчика** — определяет, как обрабатывать потоки изменений для обновления материализованного представления.

### Определение материализованного представления

```sql
CREATE ASYNC MATERIALIZED VIEW <view_name> AS
  SELECT <column_definitions>
  FROM <main_table> AS <alias>
  [<join_clauses>]
  [WHERE <filter_expression>];
```

#### Определение столбцов

Каждый столбец в предложении SELECT может быть:
- **Прямой ссылкой на столбец**: `table_alias.column_name AS column_alias`
- **Вычисляемым выражением**: `#[<yql_expression>]# AS column_alias`
- **Вычисляемым выражением со ссылками на колонки**: `COMPUTE ON table_alias.column_name, ... #[<yql_expression>]# AS column_alias`

#### Условия JOIN

```sql
[INNER | LEFT] JOIN <table_name> AS <alias>
  ON <join_condition> [AND <join_condition>]*
```

Условия JOIN поддерживают:
- Равенство столбцов: `table1.col1 = table2.col2`
- Равенство констант: `table1.col1 = 'value'` или `table1.col1 = 123`

#### Фильтрующие выражения

Предложение WHERE поддерживает непрозрачные (для приложения) YQL-выражения, подставляемые без изменений непосредственно в формируемые запросы:
```sql
WHERE COMPUTE ON table_alias.column_name, ... #[<yql_expression>]#
```

Наличие ссылок на конкретные имена таблиц и колонки позволяет корректно формировать производные SQL-операторы с использованием непрозрачных выражений, опирающихся на конкретные колонки исходных таблиц.

### Определение обработчика

```sql
CREATE ASYNC HANDLER <handler_name>
  [CONSUMER <consumer_name>]
  PROCESS <materialized_view_name>,
  [PROCESS <materialized_view_name>,]
  INPUT <table_name> CHANGEFEED <changefeed_name> AS [STREAM|BATCH],
  [INPUT <table_name> CHANGEFEED <changefeed_name> AS [STREAM|BATCH], ...];
```

#### Компоненты обработчика

- **PROCESS**: указывает, какие материализованные представления обновляет этот обработчик.
- **INPUT**: определяет входные таблицы и их потоки changefeed.
  - `STREAM`: обработка отдельных изменений в режиме реального времени.
  - `BATCH`: пакетная обработка накопленных изменений.
- **CONSUMER**: необязательное имя потребителя для changefeed (если не указано, то используется имя обработчика).

### Непрозрачные выражения

Язык поддерживает непрозрачные выражения, заключённые в разделители `#[` и `]#`. Они содержат код YQL (Yandex Query Language), который передаётся в базу данных без разбора:

```sql
-- В предложении SELECT
SELECT #[Substring(main.c20, 3, 5)]# AS c11,
       #[CAST(NULL AS Int32?)]# AS c12

-- В предложении WHERE
WHERE #[main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u)]#

-- С предложением COMPUTE ON
COMPUTE ON main, sub2 #[main.c6=7 AND (sub2.c7 IS NULL OR sub2.c7='val2'u)]#
```

### Полный пример

```sql
-- Определение материализованного представления
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

-- Определение обработчика для обработки изменений
CREATE ASYNC HANDLER h1 CONSUMER h1_consumer
  PROCESS `test1/mv1`,
  INPUT `test1/main_table` CHANGEFEED cf1 AS STREAM,
  INPUT `test1/sub_table1` CHANGEFEED cf2 AS STREAM,
  INPUT `test1/sub_table2` CHANGEFEED cf3 AS STREAM,
  INPUT `test1/sub_table3` CHANGEFEED cf4 AS BATCH;
```

### Особенности языка

- **Нечувствительность к регистру ключевых слов**: все ключевые слова SQL не чувствительны к регистру.
- **Идентификаторы в кавычках**: используйте обратные кавычки для идентификаторов со специальными символами: `` `table/name` ``.
- **Строковые литералы**: строки в одинарных кавычках с необязательными суффиксами типа (`'value'u` для Utf8).
- **Комментарии**: как однострочные комментарии (`--`), так и блочные (`/* */`).
- **Завершение точкой с запятой**: инструкции должны заканчиваться точкой с запятой.

### Поддерживаемые типы данных

Язык работает со стандартными типами данных YDB:
- **Текстовые**: строковые данные (используйте `'value'u` для строк Utf8).
- **Числовые**: Int32, Int64, Decimal и т. д.
- **Временные**: Timestamp, Date и т. д.
- **Сложные**: JsonDocument и т. д.

## Синтаксис командной строки

```bash
java -jar ydb-materializer-*.jar <config.xml> <MODE>
```

Приложение поддерживает три режима работы:
- CHECK: проверка конфигурации;
- SQL: генерация SQL-инструкций, представляющих логику материализации;
- RUN: фактическая синхронизация MV.

**Параметры:**
- `<config.xml>` — путь к файлу XML-конфигурации.
- `<MODE>` — режим работы: `CHECK`, `SQL` или `RUN`.

### Режимы работы

#### Режим CHECK
Проверяет определения материализованных представлений и сообщает о проблемах:
```bash
java -jar ydb-materializer-*.jar config.xml CHECK
```

#### Режим SQL
Генерирует и выводит SQL-инструкции для материализованных представлений:
```bash
java -jar ydb-materializer-*.jar config.xml SQL
```

#### Режим LOCAL
Запускает локальную службу обработки материализованных представлений:
```bash
java -jar ydb-materializer-*.jar config.xml LOCAL
```

#### Режим JOB
Запускает распределенную службу обработки материализованных представлений:
```bash
java -jar ydb-materializer-*.jar config.xml JOB
```

## Файл конфигурации

Файл конфигурации — это файл свойств XML, в котором определены параметры подключения и настройки задания. Вот пример конфигурации:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
<comment>Пример конфигурации YDB Materializer</comment>

<!-- *** Параметры подключения *** -->
<entry key="ydb.url">grpcs://ydb01.localdomain:2135/cluster1/testdb</entry>
<entry key="ydb.cafile">/path/to/ca.crt</entry>
<entry key="ydb.poolSize">1000</entry>
<entry key="ydb.preferLocalDc">false</entry>

<!-- Режим аутентификации: NONE, ENV, STATIC, METADATA, SAKEY -->
<entry key="ydb.auth.mode">STATIC</entry>
<entry key="ydb.auth.username">root</entry>
<entry key="ydb.auth.password">your_password</entry>

<!-- Режим ввода: FILE или TABLE -->
<entry key="job.input.mode">FILE</entry>
<entry key="job.input.file">example-job1.sql</entry>
<entry key="job.input.table">mv/statements</entry>

<!-- Конфигурация обработчика -->
<entry key="job.handlers">h1,h2,h3</entry>
<entry key="job.scan.rate">10000</entry>
<entry key="job.scan.table">mv/scans_state</entry>
<entry key="job.coordination.path">mv/coordination</entry>
<entry key="job.coordination.timeout">10</entry>

<!-- Настройки сканера справочников -->
<entry key="job.dict.consumer">dictionary</entry>
<entry key="job.dict.hist.table">mv/dict_hist</entry>
<entry key="job.dict.scan.seconds">28800</entry>

<!-- Настройка производительности -->
<entry key="job.apply.partitioning">HASH</entry>
<entry key="job.cdc.threads">4</entry>
<entry key="job.apply.threads">4</entry>
<entry key="job.apply.queue">10000</entry>
<entry key="job.batch.select">1000</entry>
<entry key="job.batch.upsert">500</entry>
<entry key="job.query.seconds">30</entry>

<!-- Настройки средств управления -->
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

### Минимальный исполняемый пример
Файлы для простого примера на одной таблице:
- `scripts/example-simple-ddl.sql` (создание таблиц и changefeed)
- `scripts/example-simple.sql` (определение MV и handler)
- `scripts/example-simple.xml` (конфиг)

### Справочная информация о параметрах конфигурации

#### Подключение к базе данных
- `ydb.url` — строка подключения к YDB (обязательно).
- `ydb.cafile` — путь к файлу TLS-сертификата (опционально).
- `ydb.poolSize` — размер пула подключений (по умолчанию: 2 × количество ядер ЦП).
- `ydb.preferLocalDc` — предпочтительный локальный центр обработки данных (по умолчанию: false).

#### Аутентификация
- `ydb.auth.mode` — режим аутентификации:
  - `NONE` — без аутентификации.
  - `ENV` — переменные окружения.
  - `STATIC` — имя пользователя и пароль.
  - `METADATA` — метаданные виртуальной машины.
  - `SAKEY` — файл ключа сервисного аккаунта.
- `ydb.auth.username` — имя пользователя (для режима STATIC).
- `ydb.auth.password` — пароль (для режима STATIC).
- `ydb.auth.sakey` — путь к файлу ключа сервисного аккаунта (для режима SAKEY).

#### Настройка задания
- `job.input.mode` — источник ввода: `FILE` или `TABLE`.
- `job.input.file` — путь к SQL-файлу (для режима FILE).
- `job.input.table` — имя таблицы для инструкций (для режима TABLE).
- `job.handlers` — список имён обработчиков для активации, разделённый запятыми.
- `job.scan.table` — имя таблицы для ведения позиций сканирования
- `job.dict.hist.table` - имя таблицы для ведения истории изменения справочников
- `job.coordination.path` — путь к узлу службы координации
- `job.coordination.timeout` - таймаут распределенной блокировки, секунд

#### Настройки сканера справочников
- `job.dict.consumer` - имя консьюмера для сбора информации об изменениях справочников
- `job.dict.hist.table` - альтернативное имя таблицы MV_DICT_HIST
- `job.dict.scan.seconds` - период между проверками изменений справочников

#### Настройка производительности
- `job.apply.partitioning` - HASH (по умолчанию) или RANGE стратегия партиционирования задач
- `job.cdc.threads` — количество потоков чтения CDC
- `job.apply.threads` — количество рабочих потоков apply
- `job.apply.queue` — максимальное количество элементов в очереди apply на поток
- `job.batch.select` — размер пакета для операций SELECT
- `job.batch.upsert` — размер пакета для операций UPSERT или DELETE
- `job.query.seconds` — максимальное время выполнения запроса на выборку, вставку или удаление данных, секунд

#### Настройки системы управления заданиями
- `mv.jobs.table` - Альтернативное имя таблицы MV_JOBS
- `mv.scans.table` - Альтернативное имя таблицы MV_JOB_SCANS
- `mv.runners.table` - Альтернативное имя таблицы MV_RUNNERS
- `mv.runner.jobs.table` - Альтернативное имя таблицы MV_RUNNER_JOBS
- `mv.commands.table` - Альтернативное имя таблицы MV_COMMANDS
- `mv.scan.period.ms` - Период сканирования Исполнителя и Координатора, миллисекунды
- `mv.report.period.ms` - Период обновления состояния Исполнителя, миллисекунды
- `mv.runner.timeout.ms` - Таймаут отсутствия обновлений Координатора и Исполнителя, миллисекунды
- `mv.coord.startup.ms` - Пауза между стартом Координатора и началом распределения заданий, миллисекунды
- `mv.coord.runners.count` - Минимальное количество Исполнителей для распределения заданий

#### Метрики
- `metrics.enabled` - Включить endpoint метрик Prometheus (по умолчанию: false)
- `metrics.port` - Порт endpoint метрик Prometheus (по умолчанию: 9090)
- `metrics.host` - Адрес/интерфейс для endpoint метрик (по умолчанию: 0.0.0.0)

Готовый стенд Prometheus + Grafana описан в `monitoring/README.md`.

## Управление распределёнными задачами (режим JOB)

Режим JOB предоставляет возможности для управления распределёнными задачами, позволяя управлять задачами обработки материализованных представлений на нескольких экземплярах. Этот режим запускается с помощью команды:

```bash
java -jar ydb-materializer.jar config.xml JOB
```

### Обзор архитектуры

Система управления распределёнными задачами состоит из двух основных компонентов:

- MvRunner — выполняет задачи локально на каждом экземпляре.
- MvCoordinator — управляет распределением и координацией задач между исполнителями.

Каждая задача — это работающий экземпляр «обработчика», определённого в конфигурации.

При запуске задачи приложение заново считывает и проверяет конфигурацию, после чего использует её в конкретной задаче.

### Управляющие таблицы

Система использует несколько таблиц YDB для управления распределёнными операциями:

#### Таблицы конфигурации

**`mv_jobs`**  — определения задач и желаемое состояние:

```sql
CREATE TABLE `mv_jobs` (
    job_name Text NOT NULL,           -- Имя обработчика
    job_settings JsonDocument,        -- Конфигурация обработчика
    should_run Bool,                  -- Должна ли задача выполняться
    PRIMARY KEY(job_name)
);
```

**`mv_job_scans`** - запросы на сканирование определённых целей:

```sql
CREATE TABLE `mv_job_scans` (
    job_name Text NOT NULL,           -- Имя обработчика
    target_name Text NOT NULL,        -- Имя целевой таблицы
    scan_settings JsonDocument,       -- Конфигурация сканирования
    requested_at Timestamp,           -- Когда был запрошен скан
    accepted_at Timestamp,            -- Когда скан был принят
    runner_id Text,                   -- Назначенный идентификатор исполнителя
    command_no Uint64,                -- Номер команды
    PRIMARY KEY(job_name, target_name)
);
```

#### Рабочие таблицы

**`mv_runners`** - активные экземпляры исполнителей:

```sql
CREATE TABLE `mv_runners` (
    runner_id Text NOT NULL,          -- Уникальный идентификатор исполнителя
    runner_identity Text,             -- Информация о хосте, PID, времени запуска
    updated_at Timestamp,             -- Последнее обновление статуса
    PRIMARY KEY(runner_id)
);
```

**`mv_runner_jobs`** - задачи, выполняемые в данный момент каждым исполнителем:

```sql
CREATE TABLE `mv_runner_jobs` (
    runner_id Text NOT NULL,          -- Идентификатор исполнителя
    job_name Text NOT NULL,           -- Имя задачи
    job_settings JsonDocument,        -- Конфигурация задачи
    started_at Timestamp,             -- Когда задача началась
    INDEX ix_job_name GLOBAL SYNC ON (job_name),
    PRIMARY KEY(runner_id, job_name)
);
```

**`mv_commands`** - очередь команд для исполнителей:

```sql
CREATE TABLE `mv_commands` (
    runner_id Text NOT NULL,          -- Целевой исполнитель
    command_no Uint64 NOT NULL,       -- Номер последовательности команды
    created_at Timestamp,             -- Время создания команды
    command_type Text,                -- START/STOP/SCAN/NOSCAN
    job_name Text,                    -- Имя целевой задачи
    target_name Text,                 -- Целевая таблица (для сканирования)
    job_settings JsonDocument,        -- Конфигурация задачи
    command_status Text,              -- CREATED/TAKEN/SUCCESS/ERROR
    command_diag Text,                -- Диагностика ошибок
    INDEX ix_command_no GLOBAL SYNC ON (command_no),
    INDEX ix_command_status GLOBAL SYNC ON (command_status, runner_id),
    PRIMARY KEY(runner_id, command_no)
);
```

### Операции управления задачами

#### Добавление задач

Чтобы добавить новую задачу, вставьте запись в таблицу `mv_jobs`:

```sql
INSERT INTO `mv_jobs` (job_name, job_settings, should_run)
VALUES ('my_handler', NULL, true);
```

Координатор автоматически обнаружит новую задачу и назначит её доступному исполнителю.

Параметры в поле `job_settings` можно не указывать (будут использованы параметры по умолчанию) или указать в виде JSON-документа следующего формата:

```json
{
    "cdcReaderThreads": 4,
    "applyThreads": 4,
    "applyQueueSize": 10000,
    "selectBatchSize": 1000,
    "upsertBatchSize": 500,
    "dictionaryScanSeconds": 28800
}
```

В примере выше показаны параметры по умолчанию для обычных задач. Для специальной задачи сканера словаря можно указать следующие параметры:

```json
{
    "upsertBatchSize": 500,
    "cdcReaderThreads": 4,
    "rowsPerSecondLimit": 10000
}
```

#### Удаление задач

Чтобы остановить и удалить задачу:

```sql
UPDATE `mv_jobs` SET should_run = false WHERE job_name = 'my_handler';
-- или
DELETE FROM `mv_jobs` WHERE job_name = 'my_handler';
```

#### Запрос на сканирование

Чтобы запросить сканирование определённой целевой таблицы:

```sql
INSERT INTO `mv_job_scans` (job_name, target_name, scan_settings, requested_at)
VALUES ('my_handler', 'target_table', '{"rowsPerSecondLimit": 5000}', CurrentUtcTimestamp());
```

### Мониторинг операций

#### Проверка выполняемых задач

```sql
SELECT rj.runner_id, rj.job_name, rj.started_at, r.runner_identity
FROM `mv_runner_jobs` rj
JOIN `mv_runners` r ON rj.runner_id = r.runner_id;
```

#### Проверка состояния задачи

```sql
SELECT j.job_name, j.should_run,
       CASE WHEN rj.job_name IS NOT NULL THEN 'RUNNING'u ELSE 'STOPPED'u END as status
FROM `mv_jobs` j
LEFT JOIN `mv_runner_jobs` rj ON j.job_name = rj.job_name;
```

#### Проверка очереди команд:

```sql
SELECT runner_id, command_no, command_type, job_name, command_status, created_at
FROM `mv_commands`
WHERE command_status IN ('CREATED'u, 'TAKEN'u)
ORDER BY created_at;
```

#### Проверка состояния исполнителя

```sql
SELECT runner_id, runner_identity, updated_at
FROM `mv_runners`
ORDER BY updated_at DESC;
```

### Типы команд

Система поддерживает четыре типа команд:

- `START` — запустить задачу на исполнителе.
- `STOP` — остановить задачу на исполнителе.
- `SCAN` — начать сканирование определённой целевой таблицы.
- `NOSCAN` — остановить уже запущенное сканирование для определённой целевой таблицы.

### Имена задач

Имя задачи для обычных задач соответствует имени обработчика. Есть два специальных имени задач:

- `ydbmv$dictionary` — сканер словаря (управляется вручную)
- `ydbmv$coordinator` — задача координатора (управляется автоматически)

Эти специальные имена нельзя использовать для обычных обработчиков (на самом деле имя обработчика не может начинаться с префикса «ydbmv»).

## Отказоустойчивость

Система обеспечивает автоматическую отказоустойчивость:

- Обнаружение сбоя исполнителя — исполнители периодически сообщают о своём состоянии; неактивные исполнители автоматически удаляются.
- Перераспределение задач — при сбое исполнителей их задачи автоматически переназначаются доступным исполнителям.
- Повтор команд — неудачные команды остаются в очереди для повторной попытки.
- Выбор лидера — одновременно активен только один экземпляр координатора.

## Развёртывание

1. **Создание управляющих таблиц** — используйте предоставленный скрипт `example-tables.sql`. Имена таблиц можно настроить по мере необходимости.
1. **Развёртывание исполнителей** — запустите несколько экземпляров в режиме JOB.
1. **Настройка задач** — вставьте определения задач в таблицу `mv_jobs`. Добавьте определения сканирования в таблицу mv_job_scans.
1. **Мониторинг операций** — используйте запросы для мониторинга, перечисленные выше.

Система автоматически распределит задачи между доступными исполнителями и поддержит желаемое состояние.
