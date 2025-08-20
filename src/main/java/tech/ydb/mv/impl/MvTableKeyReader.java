package tech.ydb.mv.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntConsumer;

import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcFlowControl;
import tech.ydb.core.grpc.GrpcReadStream;
import tech.ydb.table.Session;
import tech.ydb.table.settings.ReadTableSettings;
import tech.ydb.table.values.StructType;
import tech.ydb.table.query.ReadTablePart;

import tech.ydb.mv.YdbConnector;
import tech.ydb.mv.model.MvKeyValue;

/**
 *
 * @author zinal
 */
public class MvTableKeyReader implements MvUpdater.FlowControl {

    private static final CompletableFuture<Status> SUCCESS
            = CompletableFuture.completedFuture(Status.SUCCESS);

    private final YdbConnector conn;
    private final MvUpdater updater;
    private final String tablePath;
    private final List<String> keyColumns;
    private final StructType keyType;
    private final MvKeyValue startKey;
    private volatile GrpcReadStream<ReadTablePart> stream = null;
    private volatile CompletableFuture<Status> status = null;
    private volatile GrpcCall call = null;

    public MvTableKeyReader(MvUpdater updater, MvKeyValue startKey) {
        this.conn = updater.getConn();
        this.updater = updater;
        this.startKey = startKey;
        this.tablePath = updater.getTablePath();
        this.keyColumns = updater.getInputKeyColumns();
        this.keyType = updater.getKeyType();
    }

    public void start() {
        updater.setFlowControl(this);
        updater.start();
        try (Session session = conn.getTableClient()
                .createSession(Duration.ofSeconds(30)).join().getValue()) {
            start(session);
        }
    }

    public void cancel() {
        GrpcReadStream<ReadTablePart> theStream = stream;
        stream = null;
        if (theStream!=null) {
            theStream.cancel();
        }
    }

    public Status complete() {
        CompletableFuture<Status> theStatus = status;
        if (theStatus != null) {
            return theStatus.join();
        }
        return Status.SUCCESS;
    }

    @Override
    public void requestMessages(int count) {
        if (count > 0) {
            GrpcCall theCall = call;
            if (theCall!=null) {
                theCall.requestMessages(count);
            }
        }
    }

    private void start(Session session) {
        stream = session.executeReadTable(tablePath, makeSettings());
        stream.start(part -> handlePart(part));
    }

    private ReadTableSettings makeSettings() {
        ReadTableSettings.Builder rtsb = ReadTableSettings.newBuilder()
                .withRequestTimeout(Duration.ofHours(8))
                .withGrpcFlowControl((req) -> new GrpcCall(req))
                .orderedRead(true)
                .columns(getKeyColumns());
        if (startKey != null) {
            rtsb.fromKeyExclusive(startKey.convertKeyToTupleValue());
        }
        return rtsb.build();
    }

    private String[] getKeyColumns() {
        int count = keyType.getMembersCount();
        String[] columns = new String[count];
        for (int i=0; i<count; ++i) {
            columns[i] = keyType.getMemberName(i);
        }
        return columns;
    }

    private CompletableFuture<Status> handlePart(ReadTablePart part) {
        if (stream==null) {
            // shutdown
            return SUCCESS;
        }
        updater.addMessage(part.getResultSetReader(), null);
        return SUCCESS;
    }

    private final class GrpcCall implements GrpcFlowControl.Call {
        private final IntConsumer req;

        GrpcCall(IntConsumer req) {
            this.req = req;
            call = this;
        }

        @Override
        public void onStart() {
            req.accept(updater.getRemainingQueueCapacity());
        }

        @Override
        public void onMessageRead() {
            // nothing
        }

        void requestMessages(int count) {
            req.accept(count);
        }
    }
}
