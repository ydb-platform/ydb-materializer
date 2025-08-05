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
            return "Unknown or illegal changefeed `" + input.getChangeFeed()
                    + "` for table `" + input.getTableName()
                    + "` at " + sqlPos;
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
            return "Missing changefeed input for table `" + source.getTableName()
                    + "` in target " + target
                    + " at " + sqlPos;
        }
    }

}
