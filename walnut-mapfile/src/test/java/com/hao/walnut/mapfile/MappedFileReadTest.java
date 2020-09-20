package com.hao.walnut.mapfile;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class MappedFileReadTest {
    static File temp;
    static File data;
    static byte[] testBytes = new byte[] { 0x01, 0x02, 0x03, 0x04};
    @BeforeClass
    public static void parepare() throws IOException {
        temp = new File("./temp_read_"+ System.currentTimeMillis());
        temp.mkdirs();
        data = new File(temp.getAbsolutePath() + File.separator + "read.dat");
        RandomAccessFile randomAccessFile = new RandomAccessFile(data, "rw");
        randomAccessFile.writeInt(1);
        randomAccessFile.writeLong(2);
        randomAccessFile.write(testBytes);
        randomAccessFile.close();
    }

    @Test
    public void testRead() throws IOException {
        MappedFileConf mappedFileConf = new MappedFileConf();
        mappedFileConf.file = data;

        MappedFile mappedFile = new MappedFile(mappedFileConf);
        Assert.assertEquals((int)1, mappedFile.readInt(0));
        Assert.assertEquals(2l, mappedFile.readLong(4));
        byte[] data = new byte[testBytes.length];
        Assert.assertArrayEquals(testBytes, mappedFile.readBytes(12, data));
    }

    @AfterClass
    public static void teardown() {
        data.delete();
        temp.delete();
    }
}
