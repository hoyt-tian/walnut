package com.hao.walnut.log;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

@Slf4j
public class LogFileTest {

    protected void doTest(LogFileConf logFileConf) throws IOException {
        LogFile logFile = new LogFile(logFileConf);
        long index = logFile.append("hello".getBytes());
        Assert.assertEquals(0, index);
        index = logFile.append("world".getBytes());
        Assert.assertEquals(1, index);
        Assert.assertArrayEquals("hello".getBytes(), logFile.read(0));
        Assert.assertArrayEquals("world".getBytes(), logFile.read(1));
        logFile.close();
    }

    @Test
    public void test() throws IOException {
        LogFileConf logFileConf = new LogFileConf();
        String workspace = "./testLogFile_" + System.currentTimeMillis();
        logFileConf.setWorkspace(workspace);
        logFileConf.setFileName("test");
        doTest(logFileConf);
        File dataFile = new File(workspace + File.separator + logFileConf.fileName + ".dat");
        File indexFile = new File(workspace + File.separator + logFileConf.fileName + ".idx");
        File ws = new File(workspace);
        dataFile.delete();
        indexFile.delete();
        ws.delete();
    }
}
