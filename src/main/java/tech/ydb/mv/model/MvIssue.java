package tech.ydb.mv.model;

/**
 * Logical check issues.
 * @author zinal
 */
public interface MvIssue {

    boolean isError();

    String getMessage();

    public static abstract class Error implements MvIssue {

        @Override
        public boolean isError() {
            return true;
        }

    }

    public static abstract class Warning implements MvIssue {

        @Override
        public boolean isError() {
            return false;
        }

    }
    
}
