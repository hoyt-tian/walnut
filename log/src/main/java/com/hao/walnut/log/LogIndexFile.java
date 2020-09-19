package com.hao.walnut.log;

import com.hao.walnut.core.WALException;

import java.io.IOException;

public interface LogIndexFile extends LogFile {
    /**
     * 偏移量数据在文件中占8个字节(long类型)
     */
    long UnitSize = 8;

    /**
     * 索引文件中最多包含的索引记录上限
     */
    long MaxUnit = 1024 * 1024;

    /**
     * 返回当前索引文件的条目总数
     * @return
     */
    default long indexCount() throws IOException {
        return this.fileSize() / UnitSize;
    }



    /**
     * 从给定的索引偏移位置读取1个long，它是数据文件的实际偏移位置
     * @param indexOffset
     * @return
     * @throws IOException
     */
    long readRealOffset(long indexOffset) throws IOException;

    /**
     * 将索引文件序号转化成数据文件实际偏移
     * @param indexOffset 这是一个偏移序号，并不是索引文件的实际偏移量
     * @return
     * @throws WALException
     */
    default long dataOffset(long indexOffset) throws WALException {
        try {
            if (indexOffset > indexCount()) {
                throw new WALException("Out of bound");
            }
            return this.readRealOffset(indexOffset * UnitSize);
        } catch (IOException e) {
            e.printStackTrace();
            throw new WALException("read idx fail");
        }
    }


    /**
     * 为索引文件增加一个数据偏移量，同时返回目前索引总数
     * @param offset
     * @return
     * @throws IOException
     */
    long append(long offset) throws IOException;
}
