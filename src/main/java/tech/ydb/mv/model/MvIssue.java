package tech.ydb.mv.model;

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

    public static class UnknownInputTable extends Error {
        private final MvInput input;
        private final String tableName;

        public UnknownInputTable(MvInput input, String tableName, MvSqlPosHolder place) {
            super(place.getSqlPos());
            this.input = input;
            this.tableName = tableName;
        }

        @Override
        public String getMessage() {
            return "Unknown table `" + tableName
                    + "` in input " + input
                    + " at " + sqlPos;
        }
    }

}
