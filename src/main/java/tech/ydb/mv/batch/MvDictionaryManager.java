package tech.ydb.mv.batch;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import tech.ydb.table.query.Params;
import tech.ydb.table.values.ListValue;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructValue;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.Value;

import tech.ydb.mv.MvConfig;
import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.feeder.MvCdcAdapter;
import tech.ydb.mv.feeder.MvCdcFeeder;
import tech.ydb.mv.feeder.MvCdcSink;
import tech.ydb.mv.feeder.MvCommitHandler;
import tech.ydb.mv.data.MvChangeRecord;
import tech.ydb.mv.model.MvMetadata;
import tech.ydb.mv.model.MvDictionarySettings;
import tech.ydb.mv.model.MvInput;
import tech.ydb.mv.data.MvKey;
import tech.ydb.mv.data.YdbStruct;

/**
 * Write the changelog of the particular "dictionary" table to the journal table.
 *
 * @author zinal
 */
public class MvDictionaryManager implements MvCdcSink, MvCdcAdapter {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MvDictionaryManager.class);

    private static final Value<?> NULL_JSON = PrimitiveType.JsonDocument.makeOptional().emptyValue();

    private final MvMetadata context;
    private final YdbConnector conn;
    private final MvDictionarySettings settings;
    private final String historyTable;
    // initially stopped -> null
    private final AtomicReference<MvCdcFeeder> feeder = new AtomicReference<>();

    public MvDictionaryManager(MvMetadata context, YdbConnector conn,
            MvDictionarySettings settings) {
        this.context = context;
        this.conn = conn;
        this.settings = new MvDictionarySettings(settings);
        this.historyTable = YdbConnector.safe(settings.getHistoryTableName());
    }

    @Override
    public String getFeederName() {
        return MvConfig.DICTINARY_HANDLER;
    }

    @Override
    public int getCdcReaderThreads() {
        return settings.getThreadCount();
    }

    @Override
    public String getConsumerName() {
        return context.getDictionaryConsumer();
    }

    @Override
    public boolean isRunning() {
        return (feeder.get() != null);
    }

    public synchronized void start() {
        if (isRunning()) {
            LOG.info("Ignoring request to start an already-running dictionary manager.");
            return;
        }

        LOG.info("Starting dictionary manager.");
        MvCdcFeeder cf = new MvCdcFeeder(this, conn, this);
        feeder.set(cf);
        cf.start();
    }

    public synchronized void stop() {
        if (! isRunning()) {
            LOG.info("Ignoring request to stop an already-stopped dictionary manager.");
            return;
        }

        LOG.info("Stopping dictionary manager.");
        MvCdcFeeder cf = feeder.getAndSet(null);
        if (cf != null) {
            cf.stop();
        }
    }

    @Override
    public Collection<MvInput> getInputs() {
        return context.getHandlers().values().stream()
                .flatMap(handler -> handler.getInputs().values().stream())
                .filter(input -> input.isBatchMode() && input.isTableKnown())
                .toList();
    }

    @Override
    public boolean submit(Collection<MvChangeRecord> records, MvCommitHandler handler) {
        submitForce(records, handler);
        return true;
    }

    @Override
    public void submitForce(Collection<MvChangeRecord> records, MvCommitHandler handler) {
        if (records.size() <= settings.getUpsertBatchSize()) {
            process(records);
        } else {
            ArrayList<MvChangeRecord> input = new ArrayList<>(records);
            for (var part : Lists.partition(input, settings.getUpsertBatchSize())) {
                process(part);
            }
        }
        handler.commit(records.size());
    }

    private void process(Collection<MvChangeRecord> records) {
        StructValue[] values = records.stream()
                .map(cr -> convertRecord(cr))
                .toArray(StructValue[]::new);
        String sql = "DECLARE $input AS List<Struct<src:Text, tv:Timestamp, "
                + "key_text:Text, key_val:JsonDocument, full_val:JsonDocument>>; "
                + "UPSERT INTO `" + historyTable + "` SELECT * FROM AS_TABLE($input);";
        try {
            conn.sqlWrite(sql, Params.of("$input", ListValue.of(values)));
        } catch(Exception ex) {
            LOG.error("Failed to write dictionary batch, will raise for re-processing", ex);
            throw new RuntimeException(ex.toString());
        }
    }

    private StructValue convertRecord(MvChangeRecord cr) {
        return StructValue.of(
                "src", PrimitiveValue.newText(cr.getKey().getTableInfo().getName()),
                "tv", PrimitiveValue.newTimestamp(cr.getTv()),
                "key_text", PrimitiveValue.newText(convertKey(cr.getKey())),
                "key_val", PrimitiveValue.newJsonDocument(cr.getKey().convertKeyToJson()),
                "full_val", convertData(cr.getImageAfter())
        );
    }

    private String convertKey(MvKey key) {
        StringBuilder sb = new StringBuilder();
        for (int pos = 0; pos < key.size(); ++pos) {
            if (sb.length() > 0) {
                sb.append("|");
            }
            sb.append(key.getValue(pos).toString());
        }
        return sb.toString();
    }

    private Value<?> convertData(YdbStruct ys) {
        if (ys==null || ys.isEmpty()) {
            return NULL_JSON;
        }
        return PrimitiveValue.newJsonDocument(ys.toJson());
    }

}
