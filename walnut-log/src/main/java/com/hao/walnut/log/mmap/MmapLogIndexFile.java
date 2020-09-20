package com.hao.walnut.log.mmap;

import com.hao.walnut.core.WALException;
import com.hao.walnut.log.LogIndexFile;

import java.io.File;
import java.io.IOException;

public class MmapLogIndexFile extends MmapLogFile implements LogIndexFile {
    public MmapLogIndexFile(File file) throws WALException {
        super(file, LogIndexFile.MaxUnit * LogIndexFile.UnitSize);
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
