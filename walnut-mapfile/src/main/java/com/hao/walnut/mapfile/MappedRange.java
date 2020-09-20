package com.hao.walnut.mapfile;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MappedRange {
    long startOffset;
    int rangeSize;
    MappedByteBuffer mappedByteBuffer;

    /**
     * 写入的位置游标，不刷盘就向后移动
     */
    AtomicInteger writePosition;

    /**
     * 写入文件成功之后的确认游标
     */
    AtomicInteger commitPosition;
    FileChannel fileChannel;

    MappedRange(long startOffset, FileChannel fileChannel, int rangeSize) throws IOException {
        this.startOffset = startOffset;
        this.fileChannel = fileChannel;
        this.rangeSize = rangeSize;
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, rangeSize);
    }

    /**
     * 确保给定的位置在合法区间范围内
     * @param position
     * @param max
     */
    protected void checkPosition(int position, int max) {
        if (position < 0 || position > max) {
            throw new IndexOutOfBoundsException();
        }
    }

    /**
     * 确保读取的范围已经被commit过了
     * @param start
     * @param readBytes
     */
    protected void checkReadPosition(int start, int readBytes) {
        checkPosition(start, commitPosition.get());
        checkPosition(start + readBytes, commitPosition.get());
    }

    /**
     * 确保写入的范围在合法的区间之内
     * @param position
     * @param writeBytes
     */
    protected void checkWritePosition(int position, int writeBytes) {
//        if (position < commitPosition.get()) {
//            throw new IndexOutOfBoundsException("不能写到已经确认过的区间");
//        }
        checkPosition(position, rangeSize);
        checkPosition(position + writeBytes, rangeSize);
    }

    /**
     * 从给定位置读取字节数据
     * @param position
     * @param dst
     * @return
     */
    byte[] readBytes(int position, byte[] dst) {
        checkReadPosition(position, dst.length);
        ByteBuffer buffer = mappedByteBuffer.slice();
        buffer.position(position);
        buffer.get(dst);
        return dst;
    }

    /**
     * 写入到文件中
     * @param position
     * @param byteBuffer
     * @return
     * @throws IOException
     */
    synchronized int write(int position, ByteBuffer byteBuffer) throws IOException {
        checkWritePosition(position, byteBuffer.capacity());
        fileChannel.position(this.startOffset + position);
        int writeCount = fileChannel.write(byteBuffer);
        commitPosition.addAndGet(byteBuffer.capacity());
        return writeCount;
    }
}
