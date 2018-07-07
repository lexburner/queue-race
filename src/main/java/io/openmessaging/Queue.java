package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-07-05
 */
public class Queue {

    public final static int SINGLE_MESSAGE_SIZE = 2;

    private FileChannel channel;
    private AtomicLong wrotePosition;

    private volatile boolean firstGet = true;

    public Queue(FileChannel channel, AtomicLong wrotePosition) {
        this.channel = channel;
        this.wrotePosition = wrotePosition;
    }

    // 缓冲区大小
    public final static int bufferSize = (58 + 2) * 60;
//    public final static int bufferSize = 4*1024;

    // 写缓冲区
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    // 读缓冲区
//    private static ThreadLocal<ByteBuffer> readBufferHolder = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(bufferSize));
    private List<Block> blocks = new ArrayList<>();
    private volatile Block currentBlock;

    /**
     * put 由评测程序保证了 queue 级别的同步
     *
     * @param message
     */
    public void put(byte[] message) {
        if (currentBlock == null) {
            currentBlock = new Block();
            currentBlock.queueIndex = 0;
        }
        // 缓冲区满，先落盘
        if (message.length + SINGLE_MESSAGE_SIZE > writeBuffer.remaining()) {
            // 落盘
            flush();
        }
        writeBuffer.putShort((short) message.length);
        writeBuffer.put(message);
        currentBlock.messageSize += 1;
        currentBlock.messageLength += message.length + SINGLE_MESSAGE_SIZE;
    }

    private void flush() {
        writeBuffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        currentBlock.offset = writePosition;
        try {
            channel.write(writeBuffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeBuffer.clear();

        blocks.add(currentBlock);
        int newIndex = currentBlock.queueIndex + currentBlock.messageSize;
        currentBlock = new Block();
        currentBlock.queueIndex = newIndex;
    }

    private void flushForGet() {
        writeBuffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        currentBlock.offset = writePosition;
        try {
            channel.write(writeBuffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeBuffer.clear();
//        ((DirectBuffer) writeBuffer).cleaner().clean();
        blocks.add(currentBlock);
    }

    private java.util.Queue<PreReadMessage> preReadMessages = new ArrayDeque<>();

    /**
     * 读可能存在并发读，注意 race condition
     *
     * @param offset
     * @param num
     * @return
     */
    public synchronized Collection<byte[]> get(long offset, long num) {
        if (currentBlock == null) {
            return DefaultQueueStoreImpl.EMPTY;
        }
        if (currentBlock != null && firstGet) {
            synchronized (this) {
                if (currentBlock != null && firstGet) {
                    flushForGet();
                    firstGet = false;
                }
            }
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
        // find startBlock
        int left = 0;
        int right = blockSize - 1;
        startBlock = binarySearch(blocks, startIndex, left, right);
        endBlock = binarySearch(blocks, endIndex, startBlock, right);

        if (startBlock == -1 || endBlock == -1) {
            throw new RuntimeException("未找到对应的数据块");
        }
        List<byte[]> result = new ArrayList<>();

        // 优先从读缓冲区读
        if (preReadMessages.size() > 0) {
            System.out.println(preReadMessages.size());
            PreReadMessage head = preReadMessages.peek();
            // 非顺序读 清空缓冲区
            if (startIndex != head.index) {
                preReadMessages.clear();
            } else {
                // 存在内存缓冲区
                int preReadMessageSize = preReadMessages.size();
                for (int cnt = 0; startIndex <= endIndex && cnt < preReadMessageSize; startIndex++, cnt++) {
                    result.add(preReadMessages.poll().message);
                }
            }
        }

        if(startIndex>endIndex){
            // get all from 缓冲区
            return result;
        }
        // need read from disk
        //  startBlock need calculate again，while the endBlock stay the same
        startBlock = binarySearch(blocks, startIndex, left, right);

        for (int j = startBlock; j <= endBlock; j++) {
            Block block = blocks.get(j);
            ByteBuffer readBuffer = writeBuffer;
            readBuffer.clear();
            try {
                channel.read(readBuffer, block.offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
            readBuffer.flip();
            for (int i = 0; i < block.messageSize; i++) {
                short len = readBuffer.getShort();
                byte[] bytes = new byte[len];
                readBuffer.get(bytes);
                if (startIndex <= block.queueIndex + i && block.queueIndex + i <= endIndex) {
                    result.add(bytes);
                }
                // 最后一块考虑缓存
                if(j==endBlock && block.queueIndex + i>endIndex){
                    PreReadMessage preReadMessage = new PreReadMessage();
                    preReadMessage.index = block.queueIndex + i;
                    preReadMessage.message = bytes;
                    preReadMessages.offer(preReadMessage);
                }
            }

        }
        return result;
    }

    private int binarySearch(List<Block> blocks, int index, int left, int right) {
        while (left <= right) {//慎重截止条件，根据指针移动条件来看，这里需要将数组判断到空为止
            int mid = left + ((right - left) >> 1);//防止溢出
            Block blockItem = blocks.get(mid);
            if (blockItem.queueIndex <= index && index <= blockItem.queueIndex + blockItem.messageSize - 1) {//找到了
                index = mid;
                break;
            } else if (index < blockItem.queueIndex)
                right = mid - 1;//给定值key一定在左边，并且不包括当前这个中间值
            else
                left = mid + 1;//给定值key一定在右边，并且不包括当前这个中间值
        }

        return index;
    }

    class PreReadMessage {
        public int index;
        public byte[] message;
    }

}


