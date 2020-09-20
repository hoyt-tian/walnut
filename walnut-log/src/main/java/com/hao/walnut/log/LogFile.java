package com.hao.walnut.log;

import com.hao.walnut.mapfile.MappedFile;
import com.hao.walnut.mapfile.MappedFileConf;
import com.hao.walnut.mapfile.WriteResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class LogFile {
    MappedFile dataFile;
    MappedFile indexFile;
    LogFileConf logFileConf;
    ExecutorService executorService;

    public LogFile(LogFileConf logFileConf) throws IOException {
        this.logFileConf = logFileConf;
        this.executorService = Executors.newFixedThreadPool(logFileConf.maxThread);

        File workspace = new File(logFileConf.workspace);
        if (!workspace.exists()) {
            workspace.mkdirs();
        }

        MappedFileConf dataFileConf = new MappedFileConf();
        File dfile = new File(workspace.getAbsolutePath() + File.separator + logFileConf.getFileName() + ".dat");
        dataFileConf.setFile(dfile);

        MappedFileConf indexFileConf = new MappedFileConf();
        File ifile = new File(workspace.getAbsolutePath() + File.separator + logFileConf.getFileName() + ".idx");
        indexFileConf.setFile(ifile);


        dataFile = new MappedFile(dataFileConf);
        indexFile = new MappedFile(indexFileConf);

    }

    /**
     * 根据给定的偏移序号读取数据
     * @param offset, 相对序号，每一条写入的消息都会有对应的序号，根据这个序号可以反查消息数据
     * @return
     * @throws IOException
     */
    public byte[] read(long offset) throws IOException {
        long dataOffset = indexFile.readLong(offset * 8);
        int dataSize = dataFile.readInt(dataOffset);
        byte[] data = new byte[dataSize];
        return dataFile.readBytes(dataOffset + 4, data);
    }

    /**
     * 同步写入消息数据，写入成功时返回消息序号，根据这个序号可以再次读取消息数据
     * @param data
     * @return
     * @throws IOException
     */
    public long append(byte[] data) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + data.length);
        byteBuffer.putInt(data.length);
        byteBuffer.put(data);
        Future<WriteResponse> writeResponseFuture = dataFile.append(byteBuffer.array());
        try {
            WriteResponse writeResponse = writeResponseFuture.get();
            if (writeResponse.isSuccess()) {
                long position = writeResponse.position();
                Future<WriteResponse> indexWriteResponseFuture = indexFile.append(position);
                WriteResponse indexWriteResponse = indexWriteResponseFuture.get();
                if (indexWriteResponse.isSuccess()) {
                    return indexWriteResponse.position() / 8;
                } else {
                    throw new IOException("fail to write index");
                }
            } else {
                throw new IOException("fail to write data");
            }
        } catch (InterruptedException|ExecutionException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * 异步写入消息数据
     * @param data
     * @return
     */
    public Future<Long> appendAsync(byte[] data) {
        return executorService.submit(() ->  this.append(data));
    }

    public void close() throws IOException {
        dataFile.close();
        indexFile.close();
    }
}
