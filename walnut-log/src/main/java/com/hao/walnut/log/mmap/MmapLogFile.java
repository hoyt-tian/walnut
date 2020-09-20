package com.hao.walnut.log.mmap;

import com.hao.walnut.core.WALException;
import com.hao.walnut.log.channel.ChannelLogFile;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

@Slf4j
public class MmapLogFile extends ChannelLogFile {
    protected long maxMapSize;
    protected MappedByteBuffer mappedByteBuffer;
    protected long bufferSize = 0;
    protected long lastForce = 0;

    public MmapLogFile(File file, long maxMapSize) throws WALException {
        super(file);
        this.maxMapSize = maxMapSize;
        try {
            mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxMapSize);
            log.info("mapped file size = {}", fileSize());
        } catch (IOException e) {
            throw new WALException(e.getMessage());
        }
    }

    @Override
    public void writeBytes(long position, byte[] data) throws IOException {
        try {
            ByteBuffer byteBuffer = mappedByteBuffer.slice();
            byteBuffer.position((int)position);
            byteBuffer.put(data);
            byteBuffer.flip();
            log.info("mappedByteBufer write");
        } catch (Exception e) {
            log.error("{}", e.getMessage());
        }

        /*
        if (bufferSize >= 1024 || System.currentTimeMillis() - lastForce >= 1000 * 5) {
            lastForce = System.currentTimeMillis();
            bufferSize = 0;
            log.info("mappedByteBufer force");
        }
         */
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
//        if (realFileSize <= position) {
//            realFileSize = position + 4;
//            randomAccessFile.setLength(realFileSize);
//        }
        try {
            mappedByteBuffer.putInt((int)position, val);
        }  catch (Exception e) {
        log.error("{}", e.getMessage());
        }
    }

    @Override
    public void writeLong(long position, long val) throws IOException {
//        if (realFileSize <= position) {
//            realFileSize = position + 8;
//            randomAccessFile.setLength(realFileSize);
//        }
        try {
            mappedByteBuffer.putLong((int)position, val);
        }  catch (Exception e) {
            log.error("{}", e.getMessage());
        }
    }

    @Override
    public long readLong(long offset) throws IOException {
        return mappedByteBuffer.getLong((int)offset);
    }

}
