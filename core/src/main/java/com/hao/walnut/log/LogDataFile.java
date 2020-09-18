package com.hao.walnut.log;

import com.hao.walnut.WALException;

import java.io.IOException;

public interface LogDataFile extends LogFile {
    /**
     * 从给定数据文件的偏移量读取一个日志数据项目
     * @param start
     * @return
     * @throws WALException
     */
    LogDataItem read(long start) throws WALException;

    /**
     * 将给定数据追加到数据日志文件末尾
     * @param data
     * @return
     * @throws IOException
     */
    long append(byte[] data) throws IOException;
}
