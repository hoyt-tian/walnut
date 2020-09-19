package com.hao.walnut.log.raf;

import com.hao.walnut.core.WALException;
import com.hao.walnut.log.AbstractLogFile;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

@Slf4j
public class RafLogFile extends AbstractLogFile {
    protected RandomAccessFile randomAccessFile;

    public RafLogFile(File file) throws WALException {
        super(file);
        try {
            this.randomAccessFile = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            throw new WALException(e.getMessage());
        }
    }

    @Override
    public long fileSize() throws IOException {
        synchronized (this.randomAccessFile) {
            return this.randomAccessFile.length();
        }
    }

    @Override
    public void writeLong(long position, long val) throws IOException {
        synchronized (this.randomAccessFile) {
            this.randomAccessFile.seek(position);
            this.randomAccessFile.writeLong(val);
        }
    }

    @Override
    public long readLong(long offset) throws IOException {
        synchronized (this.randomAccessFile) {
            this.randomAccessFile.seek(offset);
            return this.randomAccessFile.readLong();
        }
    }

    @Override
    public void readBytes(long position, byte[] dest) throws IOException {
        synchronized (this.randomAccessFile) {
            this.randomAccessFile.seek(position);
            this.randomAccessFile.read(dest);
        }
    }

    @Override
    public void writeBytes(long position, byte[] data) throws IOException {
        synchronized (this.randomAccessFile) {
            this.randomAccessFile.seek(position);
            this.randomAccessFile.write(data);
        }
        log.trace("write[position={}, data.length={}]", position, data.length);
    }

    @Override
    public int readInt(long position) throws IOException {
        synchronized (this.randomAccessFile) {
            this.randomAccessFile.seek(position);
            return this.randomAccessFile.readInt();
        }
    }

    @Override
    public void writeInt(long position, int val) throws IOException {
        synchronized (this.randomAccessFile) {
            this.randomAccessFile.seek(position);
            this.randomAccessFile.writeInt(val);
        }
    }
}
