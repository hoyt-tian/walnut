package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


@Slf4j
public class MappedFile {
    protected FileChannel fileChannel;
    protected RandomAccessFile randomAccessFile;
    protected File file;
    protected List<MappedRange> mappedRangeList = new ArrayList<>();
    protected int rangeSize;
    protected long originFileSize = 0;
    protected ExecutorService writeExecutor;
    protected ExecutorService flushExecutor;
    protected Queue<WriteResponse> flushQueue;
    protected AtomicLong flushBufferCount;
    protected long lastFlushTimestamp;
    protected MappedFileConf mappedFileConf;

    public MappedFile(MappedFileConf mappedFileConf) throws IOException {
        this.mappedFileConf = mappedFileConf;
        this.file = mappedFileConf.file;
        this.randomAccessFile = new RandomAccessFile(mappedFileConf.file, "rw");
        this.originFileSize = randomAccessFile.length();
        this.fileChannel = this.randomAccessFile.getChannel();
        this.rangeSize = mappedFileConf.rangeSize;
        this.writeExecutor = Executors.newFixedThreadPool(mappedFileConf.maxWriteThread);
        this.lastFlushTimestamp = System.currentTimeMillis();

        if (mappedFileConf.flushStrategy == FlushStrategy.Batch) {
            this.flushExecutor = Executors.newSingleThreadExecutor();
            this.flushBufferCount = new AtomicLong();
            this.flushQueue = new ConcurrentLinkedQueue<>();
            this.flushExecutor.execute(this::flushTimeout);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                this.close();
            } catch (IOException e) {
                log.error("{}", e.getMessage());
            }
        }));
    }

    /**
     * 当前已经commit过的FileSize
     * @return
     */
    public long currentCommitFileSize() {
        long fileSize = 0;
        for(MappedRange mappedRange : this.mappedRangeList) {
            fileSize += mappedRange.commitPosition.get();
        }
        return fileSize;
    }


    /**
     * 当前已经预写入的FileSize
     * @return
     */
    public long currentWriteFileSize() {
        long fileSize = 0;
        for(MappedRange mappedRange : this.mappedRangeList) {
            fileSize += mappedRange.writePosition.get();
        }
        return fileSize;
    }

    public int readInt(long position) throws IOException {
        byte[] bytes = new byte[4];
        readBytes(position, bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return byteBuffer.getInt();
    }

    public long readLong(long position) throws IOException {
        byte[] bytes = new byte[8];
        readBytes(position, bytes);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return byteBuffer.getLong();
    }

    protected MappedRange lastRange() throws IOException {
        int currentSize = mappedRangeList.size();
        if (currentSize > 0) {
            return mappedRangeList.get(currentSize - 1);
        }
        return getRange(0l);
    }

    public Future<WriteResponse> append(int val) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(val);
        byteBuffer.flip();
        byte[] bytes = new byte[4];
        byteBuffer.get(bytes);
        return append(bytes);
    }

    public Future<WriteResponse> append(long val) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(val);
        byteBuffer.flip();
        byte[] bytes = new byte[8];
        byteBuffer.get(bytes);
        return append(bytes);
    }

    public synchronized Future<WriteResponse> append(byte[] val) throws IOException {
        MappedRange startRange = lastRange();
        int position = startRange.writePosition.get();
        MappedRange endRange = getRange(startRange.startOffset + position + val.length);
        WriteRequest writeRequest = new WriteRequest();

        if (startRange == endRange) {
            startRange.writePosition.getAndAdd(val.length);
            writeRequest.position = position;
            writeRequest.data = ByteBuffer.wrap(val);
            writeRequest.mappedRange = startRange;
        } else {
            startRange.writePosition.set(rangeSize);
            writeRequest.children = new LinkedList<>();
            int start = startRange.rangeSize - position;
            endRange.writePosition.set(val.length - start);
            byte[] p1 = Arrays.copyOfRange(val, 0, start);
            byte[] p2 = Arrays.copyOfRange(val, start, val.length);
            WriteRequest w1 = new WriteRequest();
            w1.position = position;
            w1.data = ByteBuffer.wrap(p1);
            w1.mappedRange = startRange;
            w1.parent = writeRequest;
            writeRequest.children.add(w1);

            WriteRequest w2 = new WriteRequest();
            w2.parent = writeRequest;
            w2.position = 0;
            w2.data = ByteBuffer.wrap(p2);
            w2.mappedRange = endRange;
            writeRequest.children.add(w2);
        }
        return execute(writeRequest);
    }

    protected WriteResponse execute(final WriteRequest writeRequest, final WriteCallback callback) {
        if (writeRequest.children == null) {
//            log.info("execute single insertion @={}, size={}", this.position + mappedRange.startOffset,data.capacity());
            WriteResponse writeResponse = new WriteResponse();
            writeResponse.writeRequest = writeRequest;
            try {
                writeResponse.writeCount = writeRequest.mappedRange.write(writeRequest.position, writeRequest.data);
                writeResponse.gmtWrite = System.currentTimeMillis();
                writeResponse.success = true;
                callback.call(writeResponse);
                return writeResponse;
            } catch (IOException e) {
                writeResponse.success = false;
                return writeResponse;
            }
        } else {
//            log.info("execute insertion with children, left=[@={},size={}], right=[@={},size={}]",left.position + left.mappedRange.startOffset, left.data.capacity(), right.position + right.mappedRange.startOffset, right.data.capacity());
            WriteResponse writeResponse = new WriteResponse();
            writeResponse.writeRequest = writeRequest;
            writeResponse.success = true;
            writeResponse.children = new LinkedList<>();
            for(WriteRequest childWriteRequest : writeRequest.children) {
                WriteResponse resp = execute(childWriteRequest, (w) -> {});
                writeResponse.success &= resp.success;
                writeResponse.children.add(resp);
                writeResponse.writeCount += resp.writeCount;
            }
            try {
                callback.call(writeResponse);
            } catch (IOException e) {
                writeResponse.success = false;
            }
            return writeResponse;
        }
    }

    protected Future<WriteResponse> execute(final WriteRequest writeRequest) {
        return writeExecutor.submit(() -> {
            WriteCallback callback = mappedFileConf.flushStrategy == FlushStrategy.Batch ? this::batchFlush : this::singleFlush;
            return execute(writeRequest, callback);
        });
    }

    protected void batchFlush(WriteResponse writeResponse) throws IOException {
        writeResponse.commitLock = new Semaphore(0);
        this.flushBufferCount.addAndGet(writeResponse.writeBytesCount());
        this.flushQueue.add(writeResponse);
        if (this.flushBufferCount.get() >= mappedFileConf.batchFlushByteSize) {
//            log.info("full in flush buffer, try to flush all of them");
            this.flush(writeResponse);
        }
        try {
            writeResponse.commitLock.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    protected void singleFlush(WriteResponse writeResponse) throws IOException {
        this.flush(writeResponse);
    }


    public byte[] readBytes(long position, byte[] data) throws IOException {
        MappedRange startRange = getRange(position);
        MappedRange endRange = getRange(position + data.length);
        if (startRange == endRange) {
            return startRange.readBytes((int)(position - startRange.startOffset), data);
        } else {
            long splitter = startRange.startOffset + rangeSize;
            int start = (int)(splitter - position);
            byte[] p1 = new byte[start];
            byte[] p2 = new byte[data.length - start];

            startRange.readBytes((int)(position - startRange.startOffset), p1);
            endRange.readBytes(0, p2);

            for(int i = 0; i < p1.length; i++) {
                data[i] = p1[i];
            }
            for(int i = 0; i < p2.length; i++) {
                data[p1.length+i] = p2[i];
            }
            return data;
        }
    }


    protected void flush(final WriteResponse writeResponse) throws IOException {
//        long beforeFlush = System.currentTimeMillis();
        fileChannel.force(false);
        this.lastFlushTimestamp = System.currentTimeMillis();
//        log.info("flush time cost = {}", lastFlushTimestamp - beforeFlush);
        if (this.mappedFileConf.flushStrategy == FlushStrategy.Batch) {
            while(this.flushQueue.size() > 0) {
                WriteResponse item = this.flushQueue.poll();
                if (item == null) break;
                item.commit();
                item.commitLock.release();
                flushBufferCount.addAndGet(-item.writeBytesCount());
                if (item == writeResponse) {
                    break;
                }
            }

//            log.info("batch flush finish");
        } else if (mappedFileConf.flushStrategy == FlushStrategy.Sync) {
            if (writeResponse != null) {
                writeResponse.commit();
            }
        } else {
            throw new IOException("Unsupported " + mappedFileConf.flushStrategy);
        }
    }

    protected static void resize(int index, List<MappedRange> rangeList) {
        int minSize = index + 1;
        if (rangeList.size() >= minSize) {
            return;
        }
        for(int i = rangeList.size(); i < minSize; i++) {
            rangeList.add(null);
        }
    }

    protected synchronized MappedRange getRange(long position) throws IOException {
        if (position < 0) {
            throw new IndexOutOfBoundsException(String.format("File=%s, position=%d",file.getAbsolutePath(), position));
        }
        int index = (int)(position / rangeSize);
        resize(index, mappedRangeList);

        MappedRange mappedRange = mappedRangeList.get(index);

        if (mappedRange != null) {
            return mappedRange;
        }
        long rangeStart = index * rangeSize;
        mappedRange = new MappedRange(rangeStart, fileChannel, rangeSize);
        mappedRange.writePosition = new AtomicInteger(
                originFileSize >= rangeStart + rangeSize ?
                        rangeSize
                        : (originFileSize > rangeStart ? (int)(originFileSize - rangeStart) : 0)
        );
        mappedRange.commitPosition = new AtomicInteger(mappedRange.writePosition.get());

        mappedRangeList.set(index, mappedRange);
        return mappedRange;
    }

    public void close() throws IOException {
        if (fileChannel.isOpen()) {
            if (mappedFileConf.flushStrategy == FlushStrategy.Batch) {
                this.flush(null);
            }
            for(MappedRange range : mappedRangeList) {
                Cleaner cl = ((DirectBuffer)range.mappedByteBuffer).cleaner();
                if (cl != null) {
                    cl.clean();
                }
            }
            fileChannel.truncate(this.currentCommitFileSize());
            fileChannel.close();
            randomAccessFile.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.close();
    }

    protected void flushTimeout() {
        while(true) {
            long duration = System.currentTimeMillis() - lastFlushTimestamp;
            if (duration > mappedFileConf.flushTimeout) {
                try {
                    log.info("flush timeout and prepare to flush");
                    this.flush(null);
                } catch (IOException e) {
                    log.error("flush when timeout fail {}", e.getMessage());
                }
            }
            long elasped = System.currentTimeMillis() - lastFlushTimestamp;
            try {
                if (elasped < mappedFileConf.flushTimeout) {
                    Thread.sleep(mappedFileConf.flushTimeout - elasped);
                }
            } catch (InterruptedException e) {
                log.error("{}", e.getMessage());
            }
        }
    }
}
