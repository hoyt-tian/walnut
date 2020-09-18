package com.hao.walnut.log;

import com.hao.walnut.WALException;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

@Slf4j
public abstract class AbstractLogFile implements LogFile{
    protected File file;

    public AbstractLogFile(File file) throws WALException {
        try {
            if (file.exists() == false) {
                this.createFile(file);
            }
            this.file = file;
        } catch (FileNotFoundException e) {
            throw new WALException("%s not exists or not writeable, %s", file.getAbsolutePath(), e.getMessage());
        } catch (IOException e) {
            throw new WALException("can not create %s, %s", file.getAbsolutePath(), e.getMessage());
        }
    }

    protected void createFile(File file) throws IOException {
        boolean r = file.createNewFile();
        if (r) {
            log.info("create {} success", file.getAbsolutePath());
        } else {
            log.error("fail to create {}", file.getAbsolutePath());
        }
    }

    public abstract void writeLong(long position, long val) throws IOException;
    public abstract long readLong(long position) throws IOException;
    public abstract void readBytes(long position, byte[] dest) throws IOException;
    public abstract void writeBytes(long position, byte[] data) throws IOException;
    public abstract int readInt(long position) throws IOException;
    public abstract void writeInt(long position, int val) throws IOException;
}
