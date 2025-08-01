package tech.ydb.mv;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import tech.ydb.common.transaction.TxMode;
import tech.ydb.mv.model.MvContext;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.model.MvIssue;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.model.MvJoinSource;
import tech.ydb.mv.model.MvTarget;
import tech.ydb.mv.parser.MvParser;
import tech.ydb.query.tools.QueryReader;
import tech.ydb.query.tools.SessionRetryContext;
import tech.ydb.table.description.TableDescription;

/**
 * Work context for YDB Materializer activities.
 * @author mzinal
 */
public class WorkContext implements AutoCloseable {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(WorkContext.class);

    private final YdbConnector connector;
    private final MvContext context;

    public WorkContext(YdbConnector.Config config) {
        this.connector = new YdbConnector(config);
        this.context = readContext(this.connector);
        refreshMetadata();
    }

    public WorkContext(YdbConnector connector) {
        this.connector = connector;
        this.context = readContext(this.connector);
        refreshMetadata();
    }

    public YdbConnector getConnector() {
        return connector;
    }

    public MvContext getContext() {
        return context;
    }

    @Override
    public void close() {
        connector.close();
    }

    private void refreshMetadata() {
        if (! context.isValid()) {
            LOG.warn("Context is not valid after parsing - metadata retrieval skipped.");
            return;
        }
        HashMap<String, MvTableInfo> info = new HashMap<>();
        for (String tabname : collectTables()) {
            MvTableInfo ti = describeTable(tabname);
            if (ti!=null) {
                info.put(tabname, ti);
            }
        }
        linkTables(info);
        validate();
    }

    private TreeSet<String> collectTables() {
        TreeSet<String> ret = new TreeSet<>();
        for (MvTarget t : context.getTargets()) {
            for (MvJoinSource r : t.getSources()) {
                ret.add(r.getTableName());
            }
        }
        for (MvInput i : context.getInputs()) {
            ret.add(i.getTableName());
        }
        return ret;
    }

    private void linkTables(HashMap<String, MvTableInfo> info) {
        for (MvTarget t : context.getTargets()) {
            for (MvJoinSource r : t.getSources()) {
                r.setTableInfo(info.get(r.getTableName()));
            }
        }
        for (MvInput i : context.getInputs()) {
            i.setTableInfo(info.get(i.getTableName()));
        }
    }

    private MvTableInfo describeTable(String tabname) {
        String path;
        if (tabname.startsWith("/")) {
            path = tabname;
        } else {
            path = connector.getDatabase() + "/" + tabname;
        }
        LOG.info("Describing table {} ...", path);
        TableDescription desc;
        try {
            desc = connector.getTableRetryCtx()
                    .supplyResult(sess -> sess.describeTable(path))
                    .join().getValue();
        } catch(Exception ex) {
            LOG.warn("Failed to obtain description for table {}", path, ex);
            return null;
        }

        MvTableInfo ret = new MvTableInfo(tabname);
        for (var c : desc.getColumns()) {
            ret.getColumns().putLast(c.getName(), c.getType());
        }
        for (String k : desc.getPrimaryKeys()) {
            ret.getKey().add(k);
        }
        for (var i : desc.getIndexes()) {
            ret.getIndexes().put(i.getName(), new ArrayList<>(i.getColumns()));
        }
        return ret;
    }

    private void validate() {
        if (! context.isValid()) {
            LOG.warn("Context already invalid, validation skipped.");
            return;
        }
        context.getTargets().forEach(t -> validate(t));
        context.getInputs().forEach(i -> validate(i));
    }

    private void validate(MvTarget t) {
        context.addIssues(t.getSources()
                .stream()
                .filter(js -> !js.isTableKnown())
                .map(js -> new MvIssue.UnknownSourceTable(t, js.getTableName(), js))
                .toList());
    }

    private void validate(MvInput i) {
        if (!i.isTableKnown()) {
            context.addIssue(new MvIssue.UnknownInputTable(i, i.getTableName(), i));
        }
    }

    private static MvContext readContext(YdbConnector ydb) {
        String mode = ydb.getProperty(App.CONF_INPUT_MODE, App.Input.FILE.name());
        switch (App.parseInput(mode)) {
            case FILE -> { return readFile(ydb); }
            case TABLE -> { return readTable(ydb); }
        }
        throw new IllegalArgumentException("Illegal value [" + mode + "] for "
                + "property " + App.CONF_INPUT_MODE);
    }

    private static MvContext readFile(YdbConnector ydb) {
        String fname = ydb.getProperty(App.CONF_INPUT_FILE, App.DEF_FILE);
        try (FileInputStream fis = new FileInputStream(fname)) {
            return new MvParser(fis, StandardCharsets.UTF_8).fill();
        } catch(IOException ix) {
            throw new RuntimeException("Failed to read file [" + fname + "]", ix);
        }
    }

    private static MvContext readTable(YdbConnector ydb) {
        String tabname = ydb.getProperty(App.CONF_INPUT_TABLE, App.DEF_TABLE);
        String sql = readStatements(ydb, tabname);
        return new MvParser(sql).fill();
    }

    private static String readStatements(YdbConnector ydb, String tabname) {
        var retryCtx = SessionRetryContext
                .create(ydb.getQueryClient())
                .idempotent(true)
                .build();
        String stmt = "SELECT statement_text, statement_no FROM `"
                + YdbConnector.safe(tabname) + "` ORDER BY statement_no";
        var result = retryCtx.supplyResult(session -> QueryReader.readFrom(
                session.createQuery(stmt, TxMode.ONLINE_RO)
        )).join().getValue().getResultSet(0);
        final StringBuilder sb = new StringBuilder();
        while (result.next()) {
            sb.append(result.getColumn(0).getText());
            sb.append("\n");
        }
        return sb.toString();
    }

}
