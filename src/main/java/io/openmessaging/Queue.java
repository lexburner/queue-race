package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-07-05
 */
public class Queue {

    public final static int SINGLE_MESSAGE_SIZE = 4;

    private FileChannel channel;
    private AtomicLong wrotePosition;
    private boolean firstGet = true;

    public Queue(FileChannel channel, AtomicLong wrotePosition) {
        this.channel = channel;
        this.wrotePosition = wrotePosition;
    }

    public final static int bufferSize = 3 * 1024;
    // queue 缓冲区
    private ByteBuffer queueBuffer = ByteBuffer.allocateDirect(bufferSize);
    private List<Block> blocks = new ArrayList<>();
    private volatile Block currentBlock;

    public void put(byte[] message) {
        synchronized (this) {
            if (currentBlock == null) {
                currentBlock = new Block();
                currentBlock.queueIndex = 0;
            }
            // 缓冲区满，先落盘
            if (message.length + SINGLE_MESSAGE_SIZE > queueBuffer.remaining()) {
                // 落盘
                flush();
            }
            queueBuffer.putInt(message.length);
            queueBuffer.put(message);
            currentBlock.messageSize += 1;
            currentBlock.messageLength += message.length + SINGLE_MESSAGE_SIZE;
        }
    }

    private void flush() {
        queueBuffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        currentBlock.offset = writePosition;
        try {
            channel.write(queueBuffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        queueBuffer.clear();

        blocks.add(currentBlock);
        int newIndex = currentBlock.queueIndex + currentBlock.messageSize;
        currentBlock = new Block();
        currentBlock.queueIndex = newIndex;
    }

    private void flushForGet() {
        queueBuffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        currentBlock.offset = writePosition;
        try {
            channel.write(queueBuffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        queueBuffer.clear();
        queueBuffer = null;
        blocks.add(currentBlock);
    }

    public Collection<byte[]> get(long offset, long num) {
        synchronized (this) {
            if (currentBlock == null) {
                return DefaultQueueStoreImpl.EMPTY;
            }
            if (currentBlock != null && firstGet) {
                flushForGet();
                firstGet = false;
            }
            int blockSize = blocks.size();
            Block lastBlock = blocks.get(blockSize - 1);
            int maxIndex = lastBlock.queueIndex + lastBlock.messageSize - 1;
            if (offset > maxIndex) {
                return DefaultQueueStoreImpl.EMPTY;
            }
            int startIndex = (int) offset;
            int endIndex = Math.min(startIndex + (int) num - 1, maxIndex);
            int startBlock = -1;
            int endBlock = -1;
            for (int i = 0; i < blockSize; i++) {
                Block blockItem = blocks.get(i);
                if (blockItem.queueIndex <= startIndex && startIndex <= blockItem.queueIndex + blockItem.messageSize - 1) {
                    startBlock = i;
                }
                if (blockItem.queueIndex <= endIndex && endIndex <= blockItem.queueIndex + blockItem.messageSize - 1) {
                    endBlock = i;
                }
            }
            if (startBlock == -1 || endBlock == -1) {
                throw new RuntimeException("未找到对应的数据块");
            }
            List<byte[]> result = new ArrayList<>();
            for (int j = startBlock; j <= endBlock; j++) {
                Block block = blocks.get(j);
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(block.messageLength);
                try {
                    channel.read(byteBuffer, block.offset);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                byteBuffer.flip();
                for (int i = 0; i < block.messageSize; i++) {
                    int len = byteBuffer.getInt();
                    byte[] bytes = new byte[len];
                    byteBuffer.get(bytes);
                    if (startIndex <= block.queueIndex + i && block.queueIndex + i <= endIndex) {
                        result.add(bytes);
                    }
                }
            }
            return result;
        }
    }


}
