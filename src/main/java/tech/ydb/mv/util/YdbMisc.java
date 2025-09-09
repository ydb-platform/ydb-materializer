package tech.ydb.mv.util;

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
        } catch(InterruptedException ix) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static boolean randomSleep(long minMillis, long maxMillis) {
        return sleep(ThreadLocalRandom.current().nextLong(minMillis, maxMillis+1));
    }

}
