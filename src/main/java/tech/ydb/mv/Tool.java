package tech.ydb.mv;

/**
 *
 * @author mzinal
 */
public class Tool implements Runnable, AutoCloseable {

    private final YdbConnector ydb;

    public Tool(YdbConnector.Config config) {
        this.ydb = new YdbConnector(config);
        init();
    }

    private void init() {

    }

    @Override
    public void close() {
        ydb.close();
    }

    public void check() {

    }

    public void showSql() {

    }

    @Override
    public void run() {

    }

}
