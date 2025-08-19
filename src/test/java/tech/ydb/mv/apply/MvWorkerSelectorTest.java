package tech.ydb.mv.apply;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ydb.table.values.PrimitiveType;

import tech.ydb.mv.model.MvKey;
import tech.ydb.mv.model.MvKeyInfo;
import tech.ydb.mv.model.MvKeyPrefix;
import tech.ydb.mv.model.MvTableInfo;
import tech.ydb.mv.util.YdbStruct;

/**
 *
 * @author zinal
 */
public class MvWorkerSelectorTest {

    private static YdbStruct YS() {
        return new YdbStruct();
    }

    private static MvKeyPrefix KP(YdbStruct ys, MvKeyInfo keyInfo) {
        return new MvKeyPrefix(ys, keyInfo);
    }

    private static MvKey KV(YdbStruct ys, MvKeyInfo keyInfo) {
        return new MvKey(ys, keyInfo);
    }

    @Test
    public void testChooser1() {
        MvTableInfo tableInfo = makeTableInfo();
        MvKeyInfo keyInfo = tableInfo.getKeyInfo();

        MvWorkerSelector.Chooser chooser = new MvWorkerSelector.Chooser(13);
        chooser.getItems().put(KP(YS().add("key1", 100), keyInfo), 0);
        chooser.getItems().put(KP(YS().add("key1", 200), keyInfo), 1);
        chooser.getItems().put(KP(YS().add("key1", 300), keyInfo), 2);
        chooser.getItems().put(KP(YS().add("key1", 400), keyInfo), 3);
        chooser.getItems().put(KP(YS().add("key1", 500), keyInfo), 4);
        chooser.getItems().put(KP(YS().add("key1", 600), keyInfo), 5);
        chooser.getItems().put(KP(YS().add("key1", 700).add("key2", 1000L), keyInfo), 6);
        chooser.getItems().put(KP(YS().add("key1", 700).add("key2", 2000L), keyInfo), 7);
        chooser.getItems().put(KP(YS().add("key1", 700).add("key2", 3000L), keyInfo), 8);
        chooser.getItems().put(KP(YS().add("key1", 700).add("key2", 4000L), keyInfo), 9);
        chooser.getItems().put(KP(YS().add("key1", 800), keyInfo), 10);
        chooser.getItems().put(KP(YS().add("key1", 900), keyInfo), 11);

        int result;

        result = chooser.choose(KV(YS().add("key1", 50).add("key2", 500L), keyInfo));
        Assertions.assertEquals(0, result);

        result = chooser.choose(KV(YS().add("key1", 100).add("key2", 500L), keyInfo));
        Assertions.assertEquals(1, result);

        result = chooser.choose(KV(YS().add("key1", 120).add("key2", 500L), keyInfo));
        Assertions.assertEquals(1, result);

        result = chooser.choose(KV(YS().add("key1", 601).add("key2", -1L), keyInfo));
        Assertions.assertEquals(6, result);

        result = chooser.choose(KV(YS().add("key1", 700).add("key2", 500L), keyInfo));
        Assertions.assertEquals(6, result);

        result = chooser.choose(KV(YS().add("key1", 700).add("key2", 1000L), keyInfo));
        Assertions.assertEquals(7, result);

        result = chooser.choose(KV(YS().add("key1", 700).add("key2", 1999L), keyInfo));
        Assertions.assertEquals(7, result);

        result = chooser.choose(KV(YS().add("key1", 700).add("key2", 2000L), keyInfo));
        Assertions.assertEquals(8, result);

        result = chooser.choose(KV(YS().add("key1", 700).add("key2", 5000L), keyInfo));
        Assertions.assertEquals(10, result);

        result = chooser.choose(KV(YS().add("key1", 800).add("key2", 5000L), keyInfo));
        Assertions.assertEquals(11, result);

        result = chooser.choose(KV(YS().add("key1", 801).add("key2", 5000L), keyInfo));
        Assertions.assertEquals(11, result);

        result = chooser.choose(KV(YS().add("key1", 901).add("key2", 5000L), keyInfo));
        Assertions.assertEquals(12, result);
    }

    @Test
    public void techSelectWorkerLoader() {
        MvWorkerSelector sw = new SW(10);
        sw.refresh();

        MvWorkerSelector.Chooser chooser = sw.getChooser();
        Assertions.assertEquals(12, chooser.getItems().size());
        System.out.println("Chooser items: " + chooser.getItems());
    }

    private static MvTableInfo makeTableInfo() {
        return MvTableInfo.newBuilder("table1")
                .addColumn("key1", PrimitiveType.Int32)
                .addColumn("key2", PrimitiveType.Int64)
                .addKey("key1")
                .addKey("key2")
                .build();
    }

    private static class SW extends MvWorkerSelector {
        public SW(int workerCount) {
            super(null, makeTableInfo(), workerCount);
        }

        @Override
        protected MvKeyPrefix[] readPrefixes() {
            MvKeyPrefix[] ret = new MvKeyPrefix[12];
            MvKeyInfo keyInfo = getKeyInfo();
            ret[0] = KP(YS().add("key1", 100), keyInfo);
            ret[1] = KP(YS().add("key1", 200), keyInfo);
            ret[2] = KP(YS().add("key1", 300), keyInfo);
            ret[3] = KP(YS().add("key1", 400), keyInfo);
            ret[4] = KP(YS().add("key1", 500), keyInfo);
            ret[5] = KP(YS().add("key1", 600).add("key2", 1000L), keyInfo);
            ret[6] = KP(YS().add("key1", 600).add("key2", 2000L), keyInfo);
            ret[7] = KP(YS().add("key1", 600).add("key2", 3000L), keyInfo);
            ret[8] = KP(YS().add("key1", 700).add("key2", 10000L), keyInfo);
            ret[9] = KP(YS().add("key1", 700).add("key2", 20000L), keyInfo);
            ret[10] = KP(YS().add("key1", 800), keyInfo);
            ret[11] = KP(YS().add("key1", 900), keyInfo);
            return ret;
        }
    }

}
