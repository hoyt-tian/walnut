package com.hao.walnut.log.mmap;

import com.hao.walnut.WALException;
import com.hao.walnut.log.channel.ChannelLogFile;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MmapLogFile extends ChannelLogFile {
    protected long maxMapSize;
    protected MappedByteBuffer mappedByteBuffer;

    public MmapLogFile(File file, long maxMapSize) throws WALException {
        super(file);
        this.maxMapSize = maxMapSize;
        try {
            mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMapSize);
        } catch (IOException e) {
            throw new WALException(e.getMessage());
        }
    }

    @Override
    public synchronized void writeBytes(long position, byte[] data) throws IOException {
        mappedByteBuffer.position((int) position);
        mappedByteBuffer.put(data);
    }

    @Override
    public synchronized void readBytes(long position, byte[] data) throws IOException {
        mappedByteBuffer.position((int)position);
        mappedByteBuffer.get(data);
    }

    @Override
    public int readInt(long position) throws IOException {
        return mappedByteBuffer.getInt((int)position);
    }

    @Override
    public void writeInt(long position, int val) throws IOException {
        mappedByteBuffer.putInt((int)position, val);
    }

    @Override
    public void writeLong(long position, long val) throws IOException {
        mappedByteBuffer.putLong((int)position, val);
    }

    @Override
    public long readLong(long offset) throws IOException {
        return mappedByteBuffer.getLong((int)offset);
    }
}
