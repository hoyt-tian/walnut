package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;


@Slf4j
public class MappedFileWriteTest {
    static File temp;

    @BeforeClass
    public static void parepare() {
        temp = new File("./temp_write_"+ System.currentTimeMillis());
        temp.mkdirs();
        log.info("创建测试目录{}", temp.getAbsolutePath());
    }

    @Test
    public void testWrite() throws IOException, ExecutionException, InterruptedException {
        File writeInt = new File(temp.getAbsoluteFile() + File.separator + "write.dat");
        MappedFileConf mappedFileConf = new MappedFileConf();
        mappedFileConf.flushStrategy = FlushStrategy.Sync;
        mappedFileConf.file = writeInt;
        MappedFile mappedFile = new MappedFile(mappedFileConf);
        int testVal = 123;
        long testlong = 789;
        byte[] testbytes = new byte[] {0x01, 0x02, 0x03, 0x04};
//        mappedFile.writeInt(0, testVal);
        mappedFile.append(testVal).get();
        log.info("写入整数测试数据");
//        mappedFile.writeLong(4, testlong);
        mappedFile.append(testlong).get();
        log.info("写入Long测试数据");
//        mappedFile.writeBytes(4 + 8, testbytes);
        mappedFile.append(testbytes).get();

        log.info("写入字节数据{}", testbytes.length);
        mappedFile.flush(null);

        mappedFile.close();
        log.info("测试文件关闭");

        mappedFile = new MappedFile(mappedFileConf);
        RandomAccessFile randomAccessFile = new RandomAccessFile(writeInt, "rw");
        randomAccessFile.seek(0);
        Assert.assertEquals(testVal, randomAccessFile.readInt());
        Assert.assertEquals(testVal, mappedFile.readInt(0));
        log.info("Write Int Success");
        randomAccessFile.seek(4);
        Assert.assertEquals(testlong, randomAccessFile.readLong());
        Assert.assertEquals(testlong, mappedFile.readLong(4));
        log.info("Write Long Success");
        byte[] readbytes = new byte[testbytes.length];
        randomAccessFile.seek(4 + 8);
        randomAccessFile.read(readbytes);
        Assert.assertArrayEquals(testbytes, readbytes);
        for(int i = 0; i < readbytes.length; i++) {
            readbytes[i] = 0x0;
        }
        mappedFile.readBytes(4 + 8, readbytes);
        Assert.assertArrayEquals(testbytes, readbytes);
        log.info("Write Bytes Success");
        randomAccessFile.close();
        mappedFile.close();
        writeInt.delete();
        log.info("测试完毕，删除临时文件");
    }

    @Test
    public void testTimeoutFlush() throws IOException, InterruptedException {
        File file = new File(temp.getAbsoluteFile() + File.separator + "timeout.dat");
        file.createNewFile();
        MappedFileConf mappedFileConf = new MappedFileConf();
        mappedFileConf.file = file;
        mappedFileConf.flushTimeout = 2000;
        MappedFile mappedFile = new MappedFile(mappedFileConf);
        mappedFile.append("hello world test here for timeout!".getBytes());
        Thread.sleep(3000);
        mappedFile.close();
        file.delete();
    }

    @AfterClass
    public static void teardown() {
        temp.delete();
        log.info("删除临时文件夹{}", temp.getAbsolutePath());
    }
}
