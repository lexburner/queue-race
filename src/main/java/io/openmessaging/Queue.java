package io.openmessaging;

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-07-05
 */
public class Queue {

    public final static int SINGLE_MESSAGE_SIZE = 1;

    private FileChannel channel;
    private AtomicLong wrotePosition;
//    private static Map<FileChannel,MappedByteBuffer> mappedByteBufferMap = new HashMap<>();

    private volatile boolean firstGet = true;
    private volatile boolean firstPut = true;

    public Queue(FileChannel channel, AtomicLong wrotePosition) {
        this.channel = channel;
        this.wrotePosition = wrotePosition;
    }

    // 缓冲区大小
//    public final static int bufferSize = (58 + 2) * 60;
    public final static int bufferSize =2*1024;

    // 写缓冲区
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    // 读缓冲区
    private static ThreadLocal<ByteBuffer> readBufferHolder = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(bufferSize));

    private static final int size = 2000 /( bufferSize / 59 );
//    private static final int size = 2000;

    private int messageSizes[] = new int[size];
    // 消息具体的长度
    private int messageLengths[] = new int[size];
    // 记录该块在物理文件中的起始偏移量
    private long offsets[] = new long[size];
    // 记录该块中第一个消息的起始消息编号
    private int queueIndexes[] = new int[size];

//    private List<Block> blocks = new ArrayList<>();
//    private volatile Block currentBlock;

    int messageSize;
    int messageLength;
    long offset;
    int queueIndex;

    int blockIndex = 0;


    /**
     * put 由评测程序保证了 queue 级别的同步
     *
     * @param message
     */
    public void put(byte[] message) {
        if(firstPut){
            this.messageSize=0;
            this.messageLength=0;
            this.queueIndex=0;
            firstPut = false;
        }
        // 缓冲区满，先落盘
        if (message.length + SINGLE_MESSAGE_SIZE > writeBuffer.remaining()) {
//            while (writeBuffer.hasRemaining()){
//                writeBuffer.put((byte)0);
//            }
            // 落盘
            flush();
        }
        writeBuffer.put((byte) message.length);
        writeBuffer.put(message);
        this.messageSize += 1;
        this.messageLength += message.length + SINGLE_MESSAGE_SIZE;
    }

    private void flush() {
        writeBuffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        this.offset = writePosition;
        try {
            channel.write(writeBuffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeBuffer.clear();


        messageSizes[blockIndex] = this.messageSize;
        messageLengths[blockIndex] = this.messageLength;
        offsets[blockIndex] = this.offset;
        queueIndexes[blockIndex] = this.queueIndex;
        blockIndex++;
        if(blockIndex>messageSizes.length * 0.9){
            copyOf(messageSizes, messageSizes.length*2);
            copyOf(messageLengths, messageLengths.length*2);
            copyOf(offsets, offsets.length*2);
            copyOf(queueIndexes, queueIndexes.length*2);
//            System.out.println("扩容"+messageSizes.length+"=》"+messageSizes.length*2);
        }


        int newIndex = this.queueIndex + this.messageSize;
        this.messageSize = 0;
        this.messageLength = 0;
        this.queueIndex = newIndex;
    }

    private void flushForGet() {
        writeBuffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        this.offset = writePosition;
        try {
            channel.write(writeBuffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeBuffer.clear();
        ((DirectBuffer) writeBuffer).cleaner().clean();

        messageSizes[blockIndex] = this.messageSize;
        messageLengths[blockIndex] = this.messageLength;
        offsets[blockIndex] = this.offset;
        queueIndexes[blockIndex] = this.queueIndex;
        blockIndex++;
    }

    /**
     * 读可能存在并发读，注意 race condition
     *
     * @param offset
     * @param num
     * @return
     */
    public Collection<byte[]> get(long offset, long num) {
        if (firstGet) {
            synchronized (this) {
                if (firstGet) {
                    flushForGet();
                    firstGet = false;
                }
            }
        }
        int maxIndex = queueIndexes[this.blockIndex -1] + messageSizes[this.blockIndex -1] - 1;
        if (offset > maxIndex) {
            return DefaultQueueStoreImpl.EMPTY;
        }
        int startIndex = (int) offset;
        int endIndex = Math.min(startIndex + (int) num - 1, maxIndex);
        int startBlock = -1;
        int endBlock = -1;
        // find startBlock
        int left = 0;
        int right = this.blockIndex -1;
        startBlock = binarySearch(startIndex, left, right);
        endBlock = binarySearch(endIndex, startBlock, right);

        if (startBlock == -1 || endBlock == -1) {
            throw new RuntimeException("未找到对应的数据块");
        }
        List<byte[]> result = new ArrayList<>();
        for (int j = startBlock; j <= endBlock; j++) {
            ByteBuffer byteBuffer = readBufferHolder.get();
            byteBuffer.clear();
            try {
                channel.read(byteBuffer, this.offsets[j]);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byteBuffer.flip();
            for (int i = 0; i < this.messageSizes[j]; i++) {
                byte len = byteBuffer.get();
                byte[] bytes = new byte[0xff & len];
                byteBuffer.get(bytes);
                if (startIndex <= this.queueIndexes[j] + i && this.queueIndexes[j] + i <= endIndex) {
                    result.add(bytes);
                }
            }
        }
        return result;
    }

    private int binarySearch(int index, int left, int right) {
        while (left <= right) {//慎重截止条件，根据指针移动条件来看，这里需要将数组判断到空为止
            int mid = left + ((right - left) >> 1);//防止溢出
            if (this.queueIndexes[mid] <= index && index <= this.queueIndexes[mid] + this.messageSizes[mid] - 1) {//找到了
                index = mid;
                break;
            } else if (index < this.queueIndexes[mid])
                right = mid - 1;//给定值key一定在左边，并且不包括当前这个中间值
            else
                left = mid + 1;//给定值key一定在右边，并且不包括当前这个中间值
        }

        return index;
    }

    public static int[] copyOf(int[] original, int newLength) {
        int[] copy = new int[newLength];
        System.arraycopy(original, 0, copy, 0,
                Math.min(original.length, newLength));
        return copy;
    }
    public static long[] copyOf(long[] original, int newLength) {
        long[] copy = new long[newLength];
        System.arraycopy(original, 0, copy, 0,
                Math.min(original.length, newLength));
        return copy;
    }


}
