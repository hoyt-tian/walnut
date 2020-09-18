package com.hao.walnut.log.channel;

import com.hao.walnut.WALException;
import com.hao.walnut.log.LogIndexFile;

import java.io.File;
import java.io.IOException;

public class ChannelLogIndexFile extends ChannelLogFile implements LogIndexFile {
    public ChannelLogIndexFile(File file) throws WALException {
        super(file);
    }

    @Override
    public long readRealOffset(long indexOffset) throws IOException {
        return super.readLong(indexOffset);
    }

    @Override
    public long append(long offset) throws IOException {
        this.writeLong(this.fileSize(), offset);
        return this.indexCount();
    }
}
