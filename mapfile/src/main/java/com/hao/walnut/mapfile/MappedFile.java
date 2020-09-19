package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
public class MappedFile {
    static final int DefaultRangeSize = 1024 * 1024 * 200;

    protected FileChannel fileChannel;
    protected RandomAccessFile randomAccessFile;
    protected File file;
    protected List<MappedRange> mappedRangeList = new ArrayList<MappedRange>();
    protected int rangeSize;
    protected long originFileSize = 0;

    public MappedFile(File file) throws IOException {
        this(file, DefaultRangeSize);
    }
    public MappedFile(File file, int rangeSzie) throws IOException {
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.originFileSize = randomAccessFile.length();
        this.fileChannel = this.randomAccessFile.getChannel();
        this.file = file;
        this.rangeSize = rangeSzie;
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
//        reentrantLock.lock();
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
            writeRequest.children.add(w1);

            WriteRequest w2 = new WriteRequest();
            w2.position = 0;
            w2.data = ByteBuffer.wrap(p2);
            w2.mappedRange = endRange;
            writeRequest.children.add(w2);
        }
//        reentrantLock.unlock();
        return writeRequest.execute();
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


    public void flush() throws IOException {
        fileChannel.force(true);
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
        long fileSize = 0;
        for(MappedRange mappedRange : this.mappedRangeList) {
            fileSize += mappedRange.commitPosition.get();
        }
        randomAccessFile.setLength(fileSize);
        log.info("fileSize cv = {}, real = {}", fileSize, randomAccessFile.length());
        fileChannel.close();
        randomAccessFile.close();
    }
}
