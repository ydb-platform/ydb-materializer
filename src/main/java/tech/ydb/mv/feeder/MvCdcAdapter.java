package tech.ydb.mv.feeder;

/**
 *
 * @author zinal
 */
public interface MvCdcAdapter {

    String getFeederName();

    int getCdcReaderThreads();

    String getConsumerName();

    boolean isRunning();

}
