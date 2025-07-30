package tech.ydb.mv.integration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.core.Status;
import tech.ydb.mv.App;
import tech.ydb.mv.YdbConnector;
import tech.ydb.query.QuerySession;

import tech.ydb.test.junit5.YdbHelperExtension;

/**
 * colima start --arch aarch64 --vm-type=vz --vz-rosetta
 * colima start --arch amd64
 *
 * @author mzinal
 */
public class BasicIntegrationTest {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(BasicIntegrationTest.class);

    private static final String CREATE_TABLES =
"""
CREATE TABLE `test1/statements` (
   statement_no Int32 NOT NULL,
   statement_text Text NOT NULL,
   PRIMARY KEY(statement_no)
);

""";

    @RegisterExtension
    private static final YdbHelperExtension YDB = new YdbHelperExtension();

    private static String getConnectionUrl() {
        StringBuilder sb = new StringBuilder();
        sb.append(YDB.useTls() ? "grpcs://" : "grpc://" );
        sb.append(YDB.endpoint());
        sb.append(YDB.database());
        return sb.toString();
    }

    private static byte[] getConfig() {
        Properties props = new Properties();
        props.setProperty("ydb.url", getConnectionUrl());
        props.setProperty("ydb.auth.mode", "NONE");
        props.setProperty(App.CONF_INPUT_MODE, App.Input.TABLE.name());
        props.setProperty(App.CONF_INPUT_TABLE, "test1/statements");

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            props.storeToXML(baos, "Test props", StandardCharsets.UTF_8);
            return baos.toByteArray();
        } catch(IOException ix) {
            throw new RuntimeException(ix);
        }
    }

    @Test
    public void test1() {
        // has to wait a bit here
        try { Thread.sleep(5000L); } catch(InterruptedException ix) {}
        // now the work
        LOG.info("Starting up...");
        YdbConnector.Config cfg = YdbConnector.Config.fromBytes(getConfig(), "config.xml", null);
        try (YdbConnector conn = new YdbConnector(cfg)) {
            fillDatabase(conn);
            LOG.info("Preparation: completed.");
        }
    }

    private void fillDatabase(YdbConnector conn) {
        runDdl(conn, CREATE_TABLES);
    }

    private void runDdl(YdbConnector conn, String sql) {
        LOG.info("Preparation: creating tables...");
        conn.getQueryRetryCtx()
                .supplyStatus(qs -> runDdl(qs, sql))
                .join()
                .expectSuccess();
    }

    private CompletableFuture<Status> runDdl(QuerySession qs, String sql) {
        return qs.createQuery(sql, TxMode.NONE)
                .execute()
                .thenApply(res -> res.getStatus());
    }

}
