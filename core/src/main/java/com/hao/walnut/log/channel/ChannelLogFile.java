package com.hao.walnut.log.channel;

import com.hao.walnut.WALException;
import com.hao.walnut.log.raf.RafLogFile;
import com.hao.walnut.util.promise.Retry;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

@Slf4j
public class ChannelLogFile extends RafLogFile {

    protected FileChannel fileChannel;

    public ChannelLogFile(File file) throws WALException {
        super(file);
        this.fileChannel = this.randomAccessFile.getChannel();
    }


    @Override
    public void writeBytes(long position, byte[] data) throws IOException {
        new Retry(() -> {
            FileLock fileLock = fileChannel.tryLock(position, data.length, false);
            if (fileLock == null) throw new Exception();
            return fileLock;
        }).then((args) -> {

            FileLock fileLock = (FileLock)args[0];
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            sizeBuffer.putInt(data.length);
            sizeBuffer.flip();
            ByteBuffer buffer = ByteBuffer.wrap(data);
            buffer.put(data);
            buffer.flip();
            try {
                fileChannel.write(sizeBuffer);
                fileChannel.write(buffer);
                fileLock.release();
//                fileChannel.force(false);
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.trace("position={}, data.length={}", position, data.length);
        });
    }

    @Override
    public void readBytes(long position, byte[] dest) throws IOException {
        new Retry(() -> {
            FileLock fileLock = fileChannel.tryLock(position, dest.length, true);
            return fileLock;
        }).then((args) -> {
            FileLock fileLock = (FileLock)args[0];
            ByteBuffer byteBuffer = ByteBuffer.wrap(dest);
            try {
                fileChannel.read(byteBuffer);
                fileLock.release();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

}
