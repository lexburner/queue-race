package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-07-10
 */
public class WriteBuffer {

    /**
     * 写入缓冲，用于刷盘
     */
    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(64 * 1024);
    private AtomicLong wrotePosition;
    private FileChannel fileChannel;
    private int bufferSize;

    public WriteBuffer(AtomicLong wrotePosition, FileChannel fileChannel) {
        this.wrotePosition = wrotePosition;
        this.fileChannel = fileChannel;
        bufferSize = 0;
    }

    /**
     * 接收每个队列的写buffer
     *
     * @param queueBuffer queue缓冲，用于冲刷大大缓冲
     */
    private synchronized long write(ByteBuffer queueBuffer) {
        long position;
        if (byteBuffer.remaining() >= queueBuffer.remaining()) {
            position = wrotePosition.get() + bufferSize;
            bufferSize += queueBuffer.remaining();
            byteBuffer.put(queueBuffer);
        } else {
            byteBuffer.flip();
            try {
                fileChannel.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            position = wrotePosition.addAndGet(bufferSize);
            bufferSize = queueBuffer.remaining();
            byteBuffer.clear();
        }
        return position;
    }
}
