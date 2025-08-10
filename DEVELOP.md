# YDB Materializer Development Guide

## Архитектурные заметки

Основные компоненты:
1. Модель данных для описания материализованного представления
2. Парсер конфигурационного формата
3. Валидатор конфигурации (+ загрузчик метаданных)
4. Распределенный планировщик для управления заданиями
5. Задание интерактивной обработки потоков изменений табличных данных
6. Задание пакетного применения изменений справочников к конкретной MV
7. Задание полной перезагрузки данных MV

### Модель данных

Более-менее готова, описана ниже в разделе "Model Classes UML Diagram". Модификации при выявлении недоделок и новых потребностей.

### Парсер конфигурационного формата

Готов, изменения при выявлении необходимых модификаций модели данных. Основан на грамматике ANLRv4.

### Валидатор конфигурации

Реализована загрузка конфигурации (псевдо-SQL-операторов) из файла и из таблицы и подача в парсер, с последующим вызовом валидатора. Валидатор включает несколько последовательно запускаемых фаз, при выявлении на очередной фазе критических ошибок дальнейшая обработка не производится.

Реализована загрузка описаний таблиц БД и привязка этих описаний к соответствующим сущностям.

Частично реализована валидация, недоделки:
- проверка состава колонок и типов данных MV
- нет автомата проверки синтаксической корректности генерируемых запросов
- корректных методов доступа при соединении таблиц в различных ситуациях

### Распределенный планировщик для управления заданиями

Планирую задействовать [db-scheduler](https://github.com/kagkarlsson/db-scheduler). Для этого требуется обеспечить работу поверх YDB (отдельный проект), за основу смотрю реализацию для JDBC (встроенный вариант) и для MongoDB.

Над стандартным планировщиком нужна надстройка, обеспечивающая запуск, остановку и контроль состояния заданий в прикладных терминах.

### Задание интерактивной обработки потоков изменений табличных данных

TODO

### Задание пакетного применения изменений справочников к конкретной MV

TODO

### Задание полной перезагрузки данных MV

TODO

## Model Classes UML Diagram

This diagram shows the relationships between all classes in the `tech.ydb.mv.model` package.

```mermaid
classDiagram
    class MvContext {
        +isValid() boolean
        +addTarget(MvTarget) void
        +addHandler(MvHandler) void
        +getTargets() Map
        +getHandlers() Map
    }

    class MvHandler {
        +getName() String
        +getInputs() ArrayList
    }

    class MvTarget {
        +getSourceByAlias(String) MvJoinSource
        +getName() String
        +getSources() ArrayList
        +getColumns() ArrayList
        +getFilter() MvComputation
        +setFilter(MvComputation)
        +addLiteral(String) MvLiteral
        +getLiteral(String) MvLiteral
        +getLiterals() Collection
    }

    class MvInput {
        +isTableKnown() boolean
        +getTableName() String
        +setTableName(String)
        +getTableInfo() MvTableInfo
        +setTableInfo(MvTableInfo)
        +getChangeFeed() String
        +setChangeFeed(String)
        +isBatchMode() boolean
        +setBatchMode(boolean)
    }

    class MvTableInfo

    class MvJoinSource {
        +isTableKnown() boolean
        +getTableName() String
        +setTableName(String)
        +getTableAlias() String
        +setTableAlias(String)
        +getMode() MvJoinMode
        +setMode(MvJoinMode)
        +getConditions() ArrayList
        +getTableInfo() MvTableInfo
        +setTableInfo(MvTableInfo)
    }

    class MvJoinMode {
        <<enumeration>>
        MAIN
        INNER
        LEFT
    }

    class MvJoinCondition

    class MvColumn {
        +isComputation() boolean
        +getName() String
        +setName(String)
        +getType() Type
        +setType(Type)
        +getSourceAlias() String
        +setSourceAlias(String)
        +getSourceColumn() String
        +setSourceColumn(String)
        +getSourceRef() MvJoinSource
        +setSourceRef(MvJoinSource)
        +getComputation() MvComputation
        +setComputation(MvComputation)
    }

    class MvComputation {
        +getExpression() String
        +setExpression(String)
        +getSources() ArrayList
    }

    class MvComputationSource

    class MvLiteral

    MvContext --> MvTarget : targets
    MvContext --> MvHandler : handlers
    MvHandler --> MvInput : inputs

    MvTarget --> MvJoinSource : sources
    MvTarget --> MvColumn : columns
    MvTarget --> MvComputation : filter
    MvTarget --> MvLiteral : literals

    MvInput --> MvTableInfo : tableInfo

    MvJoinSource --> MvJoinCondition : conditions
    MvJoinSource --> MvJoinMode : mode
    MvJoinSource --> MvTableInfo : tableInfo

    MvColumn --> MvJoinSource : sourceRef
    MvColumn --> MvComputation : computation

    MvComputation --> MvComputationSource : sources
    MvComputationSource --> MvJoinSource : reference

    MvJoinCondition --> MvJoinSource : firstRef
    MvJoinCondition --> MvJoinSource : secondRef
    MvJoinCondition --> MvLiteral : firstLiteral
    MvJoinCondition --> MvLiteral : secondLiteral
```

## Class Descriptions

### Core Classes

- **MvContext**: Root container that holds materialized view targets and handlers; provides a validity check
- **MvHandler**: Processing context that groups multiple inputs (changefeed streams)
- **MvTarget**: A materialized view definition with join sources, output columns, optional filter, and de-duplicated literals
- **MvInput**: An input table configuration with optional changefeed and discovered `MvTableInfo`; supports batch mode
- **MvJoinSource**: A table participating in a join with alias, join mode, conditions, and optional `MvTableInfo`
- **MvJoinMode**: Enumeration of join modes (`MAIN`, `INNER`, `LEFT`)
- **MvJoinCondition**: A join predicate side-by-side specification using aliases/columns and/or literals
- **MvColumn**: Output column mapping from a source or a computation; includes output type
- **MvComputation**: A computed expression with a list of source aliases (each may reference a `MvJoinSource`)
- **MvTableInfo**: Table metadata (columns, primary key, indexes, changefeeds)
- **MvLiteral**: An immutable literal value identified within a target and reused across conditions/expressions



### Key Relationships

1. **MvContext** holds all materialized view targets and handlers
2. **MvHandler** aggregates multiple **MvInput** entries
3. **MvTarget** contains **MvJoinSource** items (sources), **MvColumn** items (columns), optional **MvComputation** (filter), and manages **MvLiteral** values
4. **MvInput** links to discovered **MvTableInfo** metadata
5. **MvJoinSource** owns **MvJoinCondition** items, has a **MvJoinMode**, and may link to **MvTableInfo**
6. **MvColumn** references either a **MvJoinSource** (sourceRef) or a **MvComputation** (computation)
7. **MvComputation** has multiple sources; each computation source may reference a **MvJoinSource**
8. **MvJoinCondition** can reference two **MvJoinSource** sides and/or **MvLiteral** values
