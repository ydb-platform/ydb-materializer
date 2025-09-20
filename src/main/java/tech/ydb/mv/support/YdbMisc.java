package tech.ydb.mv.support;

import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author zinal
 */
public class YdbMisc {

    public static boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException ix) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static boolean randomSleep(long minMillis, long maxMillis) {
        return sleep(ThreadLocalRandom.current().nextLong(minMillis, maxMillis + 1));
    }

    /**
     * Convert exception to string with full stack trace.
     *
     * @param ex Exception
     * @return Formatted stack trace
     */
    public static String getStackTrace(Throwable ex) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        ex.printStackTrace(pw);
        return sw.toString();
    }

}
