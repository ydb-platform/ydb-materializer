package tech.ydb.mv.parser;

import tech.ydb.core.Status;
import tech.ydb.core.StatusCode;
import tech.ydb.core.UnexpectedResultException;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.DescribeTableSettings;

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
    public MvTableInfo describeTable(String tabname) {
        String path = ydb.fullTableName(tabname);
        LOG.debug("Describing table {} ...", path);
        TableDescription desc;
        try {
            DescribeTableSettings dts = new DescribeTableSettings();
            dts.setIncludeShardKeyBounds(true);
            desc = ydb.getTableRetryCtx()
                    .supplyResult(sess -> sess.describeTable(path, dts))
                    .join().getValue();
        } catch (Exception ex) {
            if (ex instanceof UnexpectedResultException) {
                Status status = ((UnexpectedResultException) ex).getStatus();
                if (StatusCode.SCHEME_ERROR.equals(status.getCode())) {
                    LOG.warn("Failed to obtain description for `{}` - table is missing or no access", path);
                    return null;
                }
            }
            LOG.warn("Failed to obtain description for table `{}`", path, ex);
            return null;
        }

        MvTableInfo output = new MvTableInfo(tabname, path, desc);
        grabChangefeedInfo(output);
        return output;
    }

    private void grabChangefeedInfo(MvTableInfo ti) {
        for (MvTableInfo.Changefeed cf : ti.getChangefeeds().values()) {
            String topicPath = ydb.fullCdcTopicName(ti.getName(), cf.getName());
            var topicDesc = ydb.getTopicClient().describeTopic(topicPath).join().getValue();
            for (var consumer : topicDesc.getConsumers()) {
                cf.getConsumers().add(consumer.getName());
            }
        }
    }

}
