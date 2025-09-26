package tech.ydb.mv.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.ydb.mv.MvConfig;

/**
 *
 * @author zinal
 */
public class SettingsTest {

    @Test
    public void checkDictionarySettings() {
        var src = new MvDictionarySettings();
        System.out.println("dictionarySettings: " + MvConfig.GSON.toJson(src));

        src.setRowsPerSecondLimit(600);
        src.setCdcReaderThreads(51);
        src.setUpsertBatchSize(123);

        String temp = MvConfig.GSON.toJson(src);

        var dst = MvConfig.GSON.fromJson(temp, MvDictionarySettings.class);

        Assertions.assertEquals(src, dst);
    }

    @Test
    public void checkHandlerSettings() {
        var src = new MvHandlerSettings();
        System.out.println("handlerSettings: " + MvConfig.GSON.toJson(src));

        src.setApplyQueueSize(123);
        src.setApplyThreads(456);
        src.setCdcReaderThreads(789);
        src.setDictionaryScanSeconds(512);
        src.setSelectBatchSize(789);
        src.setUpsertBatchSize(333);

        String temp = MvConfig.GSON.toJson(src);

        var dst = MvConfig.GSON.fromJson(temp, MvHandlerSettings.class);

        Assertions.assertEquals(src, dst);
    }

    @Test
    public void checkScanSettings() {
        var src = new MvScanSettings();
        System.out.println("scanSettings: " + MvConfig.GSON.toJson(src));

        src.setRowsPerSecondLimit(500);

        String temp = MvConfig.GSON.toJson(src);

        var dst = MvConfig.GSON.fromJson(temp, MvScanSettings.class);

        Assertions.assertEquals(src, dst);
    }

}
