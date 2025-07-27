# YDB Materializer Development Guide

## Model Classes UML Diagram

This diagram shows the relationships between all classes in the `tech.ydb.mv.model` package.

```mermaid
classDiagram
    class MvContext {
        -ArrayList views
        -ArrayList inputs
        -ArrayList errors
        -ArrayList warnings
        +isValid() boolean
        +addIssue(MvIssue) void
        +getViews() ArrayList
        +getInputs() ArrayList
        +getErrors() ArrayList
        +getWarnings() ArrayList
    }

    class MvTarget {
        -String name
        -ArrayList sources
        -ArrayList columns
        -MvComputation filter
        +MvTarget(MvInputPosition)
        +getSourceByName(String) MvTableRef
        +getName() String
        +setName(String)
        +getSources() ArrayList
        +getColumns() ArrayList
        +getFilter() MvComputation
        +setFilter(MvComputation)
        +toString() String
    }

    class MvInput {
        -String tableName
        -MvTableRef tableRef
        -String changeFeed
        -boolean batchMode
        +MvInput(MvInputPosition)
        +getTableName() String
        +setTableName(String)
        +getTableRef() MvTableRef
        +setTableRef(MvTableRef)
        +getChangeFeed() String
        +setChangeFeed(String)
        +isBatchMode() boolean
        +setBatchMode(boolean)
    }

    class MvTableRef {
        -String reference
        -String alias
        -Mode mode
        -ArrayList conditions
        +MvTableRef(MvInputPosition)
        +getReference() String
        +setReference(String)
        +getAlias() String
        +setAlias(String)
        +getMode() Mode
        +setMode(Mode)
        +getConditions() ArrayList
    }

    class MvTableRefMode {
        <<enumeration>>
        MAIN
        INNER
        LEFT
    }

    class MvColumn {
        -String name
        -String sourceAlias
        -String sourceColumn
        -MvTableRef sourceRef
        -MvComputation computation
        +MvColumn(MvInputPosition)
        +isComputation() boolean
        +getName() String
        +setName(String)
        +getSourceAlias() String
        +setSourceAlias(String)
        +getSourceColumn() String
        +setSourceColumn(String)
        +getSourceRef() MvTableRef
        +setSourceRef(MvTableRef)
        +getComputation() MvComputation
        +setComputation(MvComputation)
    }

    class MvComputation {
        -ArrayList sources
        -String expression
        +MvComputation(MvInputPosition)
        +getExpression() String
        +setExpression(String)
        +getSources() ArrayList
    }

    class MvComputationSource {
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
    }

    class MvIssue {
        <<interface>>
        +isError() boolean
        +getMessage() String
        +getPosition() MvInputPosition
    }

    MvContext --> MvTarget : contains
    MvContext --> MvInput : contains
    MvContext --> MvIssue : contains

    MvTarget --> MvTableRef : sources
    MvTarget --> MvColumn : columns
    MvTarget --> MvComputation : filter

    MvInput --> MvTableRef : tableRef

    MvTableRef --> MvJoinCondition : conditions
    MvTableRef --> MvTableRefMode : mode

    MvColumn --> MvTableRef : sourceRef
    MvColumn --> MvComputation : computation

    MvComputation --> MvComputationSource : sources
    MvComputationSource --> MvTableRef : reference

    MvJoinCondition --> MvTableRef : firstRef
    MvJoinCondition --> MvTableRef : secondRef


```

## Class Descriptions

### Core Classes

- **MvContext**: The main container class that holds all materialized view definitions, inputs, and validation issues with issue management
- **MvTarget**: Represents a materialized view target with its sources, columns, and optional filter, includes source lookup functionality
- **MvInput**: Represents an input table with optional change feed configuration
- **MvTableRef**: Represents a table reference in joins with its alias and join mode
- **MvColumn**: Represents a column definition, either from a source or computed, with enhanced source reference tracking
- **MvComputation**: Represents computed expressions with their sources
- **MvJoinCondition**: Represents join conditions between tables
- **MvIssue**: Interface for validation errors and warnings with position tracking



### Key Relationships

1. **MvContext** is the root container that holds all materialized view definitions and manages issue collection
2. **MvTarget** represents individual materialized views with enhanced source lookup capabilities
3. **MvTableRef** represents table references in joins with their conditions
4. **MvColumn** can reference either a source column or a computation, with enhanced source reference tracking
5. **MvComputation** can have multiple sources and represents computed expressions
6. **MvIssue** provides a comprehensive hierarchy for error and warning reporting with position tracking 