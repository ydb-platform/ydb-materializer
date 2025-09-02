package tech.ydb.mv.model;

import java.util.List;

/**
 * Logical check issues.
 * @author zinal
 */
public interface MvIssue extends MvSqlPosHolder {

    boolean isError();

    String getMessage();

    public static abstract class Issue implements MvIssue {
        final MvSqlPos sqlPos;

        public Issue(MvSqlPos mip) {
            this.sqlPos = mip;
        }

        @Override
        public MvSqlPos getSqlPos() {
            return sqlPos;
        }

        @Override
        public String toString() {
            return getMessage();
        }
    }

    public static abstract class Error extends Issue {
        public Error(MvSqlPos mip) {
            super(mip);
        }

        @Override
        public boolean isError() {
            return true;
        }
    }

    public static abstract class Warning extends Issue {
        public Warning(MvSqlPos mip) {
            super(mip);
        }

        @Override
        public boolean isError() {
            return false;
        }
    }

    public static class LexerError extends Error {
        private final String msg;

        public LexerError(int row, int column, String msg) {
            super(new MvSqlPos(row, column));
            this.msg = msg;
        }

        @Override
        public String getMessage() {
            return "Lexer error at " + sqlPos + ": " + msg;
        }
    }

    public static class ParserError extends Error {
        private final String msg;

        public ParserError(int row, int column, String msg) {
            super(new MvSqlPos(row, column));
            this.msg = msg;
        }

        @Override
        public String getMessage() {
            return "Parser error at " + sqlPos + ": " + msg;
        }
    }

    public static class UnknownAlias extends Error {
        private final MvTarget target;
        private final String aliasName;

        public UnknownAlias(MvTarget target, String aliasName, MvSqlPosHolder place) {
            super(place.getSqlPos());
            this.target = target;
            this.aliasName = aliasName;
        }

        @Override
        public String getMessage() {
            return "Cannot resolve alias `" + aliasName
                    + "` in target " + target
                    + " at " + sqlPos;
        }
    }

    public static class IllegalJoinCondition extends Error {
        private final MvTarget target;
        private final MvJoinSource src;
        private final MvJoinCondition cond;

        public IllegalJoinCondition(MvTarget target, MvJoinSource src, MvJoinCondition cond) {
            super(cond.getSqlPos());
            this.target = target;
            this.src = src;
            this.cond = cond;
        }

        @Override
        public String getMessage() {
            return "Illegal join condition on source alias " + src.getTableAlias()
                    + " in target " + target
                    + " at " + sqlPos;
        }
    }

    public static class MissingTargetTable extends Error {
        private final MvTarget target;

        public MissingTargetTable(MvTarget target) {
            super(target.getSqlPos());
            this.target = target;
        }

        @Override
        public String getMessage() {
            return "Missing output table for target `" + target.getName()
                    + " at " + sqlPos;
        }
    }

    public static class UnknownSourceTable extends Error {
        private final MvTarget target;
        private final String tableName;

        public UnknownSourceTable(MvTarget target, String tableName, MvSqlPosHolder place) {
            super(place.getSqlPos());
            this.target = target;
            this.tableName = tableName;
        }

        @Override
        public String getMessage() {
            return "Unknown table `" + tableName
                    + "` in target " + target
                    + " at " + sqlPos;
        }
    }

    public static class MismatchedSourceTable extends Error {
        private final MvTarget target;
        private final MvJoinSource js;

        public MismatchedSourceTable(MvTarget target, MvJoinSource js) {
            super(js.getSqlPos());
            this.target = target;
            this.js = js;
        }

        @Override
        public String getMessage() {
            if (js.getTableInfo()==null) {
                return "Missing source table information `" + js.getTableName()
                        + "` in target " + target
                        + " at " + sqlPos;
            } else {
                return "Mismatched source table `" + js.getTableName()
                        + "` vs `" + js.getTableInfo().getName()
                        + "` in target " + target
                        + " at " + sqlPos;
            }
        }
    }

    public static class UnknownColumnInCondition extends Error {
        private final MvTarget target;
        private final MvJoinCondition cond;
        private final String tableAlias;
        private final String columnName;

        public UnknownColumnInCondition(MvTarget target, MvJoinCondition cond,
                String tableAlias, String columnName) {
            super(cond.getSqlPos());
            this.target = target;
            this.cond = cond;
            this.tableAlias = tableAlias;
            this.columnName = columnName;
        }

        @Override
        public String getMessage() {
            return "Unknown column `" + columnName
                    + "` referenced for alias `" + tableAlias
                    + "` in target `" + target.getName()
                    + "` at " + cond.getSqlPos();
        }
    }

    public static class UnknownOutputColumn extends Error {
        private final MvTarget target;
        private final MvColumn column;

        public UnknownOutputColumn(MvTarget target, MvColumn column) {
            super(column.getSqlPos());
            this.target = target;
            this.column = column;
        }

        @Override
        public String getMessage() {
            return "Unknown output column `" + column.getName()
                    + "` in target `" + target.getName()
                    + "` at " + sqlPos;
        }
    }

    public static class IllegalOutputReference extends Error {
        private final MvTarget target;
        private final MvColumn column;

        public IllegalOutputReference(MvTarget target, MvColumn column) {
            super(column.getSqlPos());
            this.target = target;
            this.column = column;
        }

        @Override
        public String getMessage() {
            return "Illegal column reference `" + column.getSourceColumn()
                    + "` by alias `" + column.getSourceAlias()
                    + "` for output column `" + column.getName()
                    + "` in target `" + target.getName()
                    + "` at " + sqlPos;
        }
    }

    public static class UnknownInputTable extends Error {
        private final MvInput input;

        public UnknownInputTable(MvInput input) {
            super(input.getSqlPos());
            this.input = input;
        }

        @Override
        public String getMessage() {
            return "Unknown table `" + input.getTableName()
                    + "` at " + sqlPos;
        }
    }

    public static class UnknownChangefeed extends Error {
        private final MvInput input;

        public UnknownChangefeed(MvInput input) {
            super(input.getSqlPos());
            this.input = input;
        }

        @Override
        public String getMessage() {
            return "Unknown or illegal changefeed `" + input.getChangefeed()
                    + "` for table `" + input.getTableName()
                    + "` at " + sqlPos;
        }
    }

    public static class DuplicateTarget extends Error {
        private final MvTarget cur;
        private final MvTarget prev;

        public DuplicateTarget(MvTarget cur, MvTarget prev) {
            super(cur.getSqlPos());
            this.cur = cur;
            this.prev = prev;
        }

        @Override
        public String getMessage() {
            return "Duplicate target `" + cur.getName()
                    + "` at " + sqlPos + ", already defined at "
                    + prev.getSqlPos();
        }
    }

    public static class UnknownTarget extends Error {
        private final MvHandler handler;
        private final String name;

        public UnknownTarget(MvHandler handler, String name) {
            super(handler.getSqlPos());
            this.handler = handler;
            this.name = name;
        }

        @Override
        public String getMessage() {
            return "Reference to undefined target `" + name
                    + "` in handler `" + handler.getName()
                    + "` at " + sqlPos;
        }
    }

    public static class DuplicateHandler extends Error {
        private final MvHandler cur;
        private final MvHandler prev;

        public DuplicateHandler(MvHandler cur, MvHandler prev) {
            super(cur.getSqlPos());
            this.cur = cur;
            this.prev = prev;
        }

        @Override
        public String getMessage() {
            return "Duplicate handler `" + cur.getName()
                    + "` at " + sqlPos + ", already defined at "
                    + prev.getSqlPos();
        }
    }

    public static class DuplicateInput extends Error {
        private final MvInput cur;
        private final MvInput prev;

        public DuplicateInput(MvInput cur, MvInput prev) {
            super(cur.getSqlPos());
            this.cur = cur;
            this.prev = prev;
        }

        @Override
        public String getMessage() {
            return "Duplicate input for table `" + cur.getTableName()
                    + "` at " + sqlPos + ", already defined at "
                    + prev.getSqlPos();
        }
    }

    public static class UselessInput extends Warning {
        private final MvInput input;

        public UselessInput(MvInput input) {
            super(input.getSqlPos());
            this.input = input;
        }

        @Override
        public String getMessage() {
            return "Useless input for table `" + input.getTableName()
                    + "` at " + sqlPos;
        }
    }

    public static class MissingInput extends Warning {
        private final MvTarget target;
        private final MvJoinSource source;

        public MissingInput(MvTarget target, MvJoinSource source) {
            super(source.getSqlPos());
            this.target = target;
            this.source = source;
        }

        @Override
        public String getMessage() {
            return "Missing changefeed for table `" + source.getTableName()
                    + "` used as `" + source.getTableAlias()
                    + "` in target " + target
                    + " at " + sqlPos;
        }
    }

    public static class EmptyHandler extends Error {
        private final MvHandler handler;
        private final EmptyHandlerType type;

        public EmptyHandler(MvHandler handler, EmptyHandlerType type) {
            super(handler.getSqlPos());
            this.handler = handler;
            this.type = type;
        }

        private String getTypeExplanation() {
            switch (type) {
                case NO_TARGETS: return "no targets";
                case NO_INPUTS: return "no inputs";
                default: return "something";
            }
        }

        @Override
        public String getMessage() {
            return "Empty handler `" + handler.getName()
                    + "`: " + getTypeExplanation()
                    + " at " + sqlPos;
        }
    }

    public static enum EmptyHandlerType {
        NO_TARGETS,
        NO_INPUTS
    }

    public static class TargetMultipleHandlers extends Error {
        private final MvTarget target;
        private final MvHandler handler1;
        private final MvHandler handler2;

        public TargetMultipleHandlers(MvTarget target, MvHandler handler1, MvHandler handler2) {
            super(handler2.getSqlPos());
            this.target = target;
            this.handler1 = handler1;
            this.handler2 = handler2;
        }

        @Override
        public String getMessage() {
            return "Target `" + target.getName()
                    + "` referenced by handler `" + handler2.getName()
                    + "` at " + handler2.getSqlPos()
                    + "` is also referenced by handler `" + handler1.getName()
                    + "` at " + handler1.getSqlPos();
        }
    }

    public static class UselessTarget extends Warning {
        private final MvTarget target;

        public UselessTarget(MvTarget target) {
            super(target.getSqlPos());
            this.target = target;
        }

        @Override
        public String getMessage() {
            return "Target `" + target.getName()
                    + "` at " + sqlPos
                    + "` is not used in any handler.";
        }
    }

    public static class KeyExtractionImpossible extends Warning {
        private final MvTarget target;
        private final MvJoinSource source;

        public KeyExtractionImpossible(MvTarget target, MvJoinSource source) {
            super(source.getSqlPos());
            this.target = target;
            this.source = source;
        }

        @Override
        public String getMessage() {
            return "Key extraction is not possible "
                    + " for table `" + source.getTableName()
                    + "` used as alias `" + source.getTableAlias()
                    + "` in target `" + target.getName()
                    + "` at " + sqlPos;
        }
    }

    public static class MissingJoinIndex extends Warning {
        private final MvTarget target;
        private final MvJoinSource source;
        private final List<String> columns;

        public MissingJoinIndex(MvTarget target, MvJoinSource source, List<String> columns) {
            super(source.getSqlPos());
            this.target = target;
            this.source = source;
            this.columns = new java.util.ArrayList<>(columns);
        }

        @Override
        public String getMessage() {
            return "Missing index on columns " + columns
                    + " for table `" + source.getTableName()
                    + "` used as alias `" + source.getTableAlias()
                    + "` in target `" + target.getName()
                    + "` at " + sqlPos;
        }
    }

}
