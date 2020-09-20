package com.hao.walnut.log.raf;

import com.hao.walnut.core.WALException;
import com.hao.walnut.log.LogIndexFile;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.IOException;

/**
 * 只存数据开始的索引位置
 */
@Slf4j
public class RafLogIndexFile extends RafLogFile implements LogIndexFile {

    public RafLogIndexFile(File file) throws WALException {
        super(file);
    }

    @Override
    public long readRealOffset(long indexOffset) throws IOException {
        return this.readLong(indexOffset);
    }

    @Override
    public long append(long offset) throws IOException {
        this.writeLong(this.fileSize(), offset);
        return this.indexCount();
    }

}
