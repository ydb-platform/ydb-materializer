# YDB Materializer Development Guide

## Model Classes UML Diagram

This diagram shows the relationships between all classes in the `tech.ydb.mv.model` package.

```mermaid
classDiagram
    class MvContext {
        -ArrayList~MvTarget~ views
        -ArrayList~MvInput~ inputs
        -ArrayList~MvIssue~ errors
        -ArrayList~MvIssue~ warnings
        +isValid() boolean
        +getViews() ArrayList~MvTarget~
        +getInputs() ArrayList~MvInput~
        +getErrors() ArrayList~MvIssue~
        +getWarnings() ArrayList~MvIssue~
    }

    class MvTarget {
        -String name
        -ArrayList~MvTableRef~ sources
        -ArrayList~MvColumn~ columns
        -MvComputation filter
        -MvInputPosition inputPosition
        +MvTarget(MvInputPosition)
        +getName() String
        +setName(String)
        +getSources() ArrayList~MvTableRef~
        +getColumns() ArrayList~MvColumn~
        +getFilter() MvComputation
        +setFilter(MvComputation)
        +getInputPosition() MvInputPosition
        +setInputPosition(MvInputPosition)
    }

    class MvInput {
        -String tableName
        -MvTableRef tableRef
        -String changeFeed
        -boolean batchMode
        -MvInputPosition inputPosition
        +MvInput(MvInputPosition)
        +getTableName() String
        +setTableName(String)
        +getTableRef() MvTableRef
        +setTableRef(MvTableRef)
        +getChangeFeed() String
        +setChangeFeed(String)
        +isBatchMode() boolean
        +setBatchMode(boolean)
        +getInputPosition() MvInputPosition
        +setInputPosition(MvInputPosition)
    }

    class MvTableRef {
        -String reference
        -String alias
        -Mode mode
        -ArrayList~MvJoinCondition~ conditions
        -MvInputPosition inputPosition
        +MvTableRef(MvInputPosition)
        +getReference() String
        +setReference(String)
        +getAlias() String
        +setAlias(String)
        +getMode() Mode
        +setMode(Mode)
        +getConditions() ArrayList~MvJoinCondition~
        +getInputPosition() MvInputPosition
        +setInputPosition(MvInputPosition)
    }

    class MvTableRef.Mode {
        <<enumeration>>
        MAIN
        INNER
        LEFT
    }

    class MvColumn {
        -String name
        -String sourceAlias
        -String sourceColumn
        -MvComputation computation
        -MvInputPosition inputPosition
        +MvColumn(MvInputPosition)
        +isComputation() boolean
        +getName() String
        +setName(String)
        +getSourceAlias() String
        +setSourceAlias(String)
        +getSourceColumn() String
        +setSourceColumn(String)
        +getComputation() MvComputation
        +setComputation(MvComputation)
        +getInputPosition() MvInputPosition
        +setInputPosition(MvInputPosition)
    }

    class MvComputation {
        -ArrayList~Source~ sources
        -String expression
        -MvInputPosition inputPosition
        +MvComputation(MvInputPosition)
        +getExpression() String
        +setExpression(String)
        +getSources() ArrayList~Source~
        +getInputPosition() MvInputPosition
        +setInputPosition(MvInputPosition)
    }

    class MvComputation.Source {
        -String alias
        -MvTableRef reference
        +Source(String)
        +Source(String, MvTableRef)
        +getAlias() String
        +setAlias(String)
        +getReference() MvTableRef
        +setReference(MvTableRef)
    }

    class MvJoinCondition {
        -String firstLiteral
        -MvTableRef firstRef
        -String firstAlias
        -String firstColumn
        -String secondLiteral
        -MvTableRef secondRef
        -String secondAlias
        -String secondColumn
        -MvInputPosition inputPosition
        +MvJoinCondition(MvInputPosition)
        +getFirstLiteral() String
        +setFirstLiteral(String)
        +getFirstRef() MvTableRef
        +setFirstRef(MvTableRef)
        +getFirstAlias() String
        +setFirstAlias(String)
        +getFirstColumn() String
        +setFirstColumn(String)
        +getSecondLiteral() String
        +setSecondLiteral(String)
        +getSecondRef() MvTableRef
        +setSecondRef(MvTableRef)
        +getSecondAlias() String
        +setSecondAlias(String)
        +getSecondColumn() String
        +setSecondColumn(String)
        +getInputPosition() MvInputPosition
        +setInputPosition(MvInputPosition)
    }

    class MvInputPosition {
        -int line
        -int column
        +MvInputPosition(int, int)
        +getLine() int
        +getColumn() int
    }

    class MvIssue {
        <<interface>>
        +isError() boolean
        +getMessage() String
    }

    class MvIssue.Error {
        <<abstract>>
        +isError() boolean
    }

    class MvIssue.Warning {
        <<abstract>>
        +isError() boolean
    }

    %% Relationships
    MvContext ||--o{ MvTarget : contains
    MvContext ||--o{ MvInput : contains
    MvContext ||--o{ MvIssue : contains

    MvTarget ||--o{ MvTableRef : sources
    MvTarget ||--o{ MvColumn : columns
    MvTarget ||--o| MvComputation : filter

    MvInput ||--o| MvTableRef : tableRef

    MvTableRef ||--o{ MvJoinCondition : conditions
    MvTableRef ||--|| MvTableRef.Mode : mode

    MvColumn ||--o| MvComputation : computation

    MvComputation ||--o{ MvComputation.Source : sources
    MvComputation.Source ||--o| MvTableRef : reference

    MvJoinCondition ||--o| MvTableRef : firstRef
    MvJoinCondition ||--o| MvTableRef : secondRef

    MvIssue <|-- MvIssue.Error
    MvIssue <|-- MvIssue.Warning

    %% All classes have inputPosition
    MvTarget ||--|| MvInputPosition : inputPosition
    MvInput ||--|| MvInputPosition : inputPosition
    MvTableRef ||--|| MvInputPosition : inputPosition
    MvColumn ||--|| MvInputPosition : inputPosition
    MvComputation ||--|| MvInputPosition : inputPosition
    MvJoinCondition ||--|| MvInputPosition : inputPosition
```

## Class Descriptions

### Core Classes

- **MvContext**: The main container class that holds all materialized view definitions, inputs, and validation issues
- **MvTarget**: Represents a materialized view target with its sources, columns, and optional filter
- **MvInput**: Represents an input table with optional change feed configuration
- **MvTableRef**: Represents a table reference in joins with its alias and join mode
- **MvColumn**: Represents a column definition, either from a source or computed
- **MvComputation**: Represents computed expressions with their sources
- **MvJoinCondition**: Represents join conditions between tables
- **MvInputPosition**: Tracks the position in the input for error reporting
- **MvIssue**: Interface for validation errors and warnings

### Key Relationships

1. **MvContext** is the root container that holds all materialized view definitions
2. **MvTarget** represents individual materialized views with their sources and columns
3. **MvTableRef** represents table references in joins with their conditions
4. **MvColumn** can reference either a source column or a computation
5. **MvComputation** can have multiple sources and represents computed expressions
6. All model classes track their input position for error reporting
7. **MvIssue** provides a hierarchy for error and warning reporting 