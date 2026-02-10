package tech.ydb.mv.parser;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.svc.MvConnector;

/**
 *
 * @author zinal
 */
public class MvDescriberYdb implements MvDescriber {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvDescriberYdb.class);

    private final YdbConnector ydb;

    public MvDescriberYdb(YdbConnector ydb) {
        this.ydb = ydb;
    }

    @Override
    public YdbConnector getYdb() {
        return ydb;
    }

    @Override
    public MvTableInfo describeTable(String table, String destination) {
        MvConnector conn;
        tech.ydb.table.SessionRetryContext retryCtx;
        if (destination == null || destination.length() == 0) {
            conn = ydb.getConnStd();
            retryCtx = ydb.getTableRetryCtx();
        } else {
            var connExt = ydb.getConnExt(destination);
            conn = connExt;
            retryCtx = connExt.getTableRetryCtx();
        }

        String path = conn.fullTableName(table);
        LOG.debug("Describing table {} ...", path);

        TableDescription desc;
        try {
            DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            desc = retryCtx.supplyResult(sess -> sess.describeTable(path, dts))
                    .join().getValue();
        } catch (Exception ex) {
            if (ex instanceof UnexpectedResultException) {
                Status status = ((UnexpectedResultException) ex).getStatus();
                if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                    LOG.error("Failed to obtain description for `{}` - table is missing or no access", path);
                    return null;
                }
            }
            LOG.error("Failed to obtain description for table `{}`", path, ex);
            return null;
        }

        MvTableInfo output = new MvTableInfo(table, path, desc);
        if (conn == ydb.getConnStd()) {
            grabChangefeedInfo(conn, output);
        }
        return output;
    }

    private void grabChangefeedInfo(MvConnector conn, MvTableInfo ti) {
        for (MvTableInfo.Changefeed cf : ti.getChangefeeds().values()) {
            String topicPath = conn.fullCdcTopicName(ti.getName(), cf.getName());
            var topicDesc = ydb.getTopicClient().describeTopic(topicPath).join().getValue();
            for (var consumer : topicDesc.getConsumers()) {
                cf.getConsumers().add(consumer.getName());
            }
        }
    }

}
