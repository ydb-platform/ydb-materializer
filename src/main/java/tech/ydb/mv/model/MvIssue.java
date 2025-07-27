package tech.ydb.mv.model;

/**
 * Logical check issues.
 * @author zinal
 */
public interface MvIssue {

    boolean isError();

    String getMessage();

    MvInputPosition getPosition();

    public static abstract class Issue implements MvIssue {
        final MvInputPosition mip;

        public Issue(MvInputPosition mip) {
            this.mip = mip;
        }

        @Override
        public MvInputPosition getPosition() {
            return mip;
        }
    }

    public static abstract class Error extends Issue {
        public Error(MvInputPosition mip) {
            super(mip);
        }

        @Override
        public boolean isError() {
            return true;
        }
    }

    public static abstract class Warning extends Issue {
        public Warning(MvInputPosition mip) {
            super(mip);
        }

        @Override
        public boolean isError() {
            return false;
        }
    }

    public static class UnknownAlias extends Error {

        private final MvTarget target;
        private final String aliasName;

        public UnknownAlias(MvTarget target, String aliasName, MvPositionHolder place) {
            super(place.getInputPosition());
            this.target = target;
            this.aliasName = aliasName;
        }

        @Override
        public String getMessage() {
            return "Cannot resolve alias `" + aliasName
                    + "` in target " + target
                    + " at " + mip;
        }

    }

}
