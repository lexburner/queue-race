package io.openmessaging;

import sun.nio.ch.DirectBuffer;

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

    public final static int SINGLE_MESSAGE_SIZE = 58;
    public final static int BLOCK_SIZE = 40;

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
    public final static int bufferSize = SINGLE_MESSAGE_SIZE * BLOCK_SIZE;

    // 写缓冲区
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    // 读缓冲区
    private static ThreadLocal<ByteBuffer> readBufferHolder = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(bufferSize));

    private static final int size = 2000 / BLOCK_SIZE + 1;
//    private static final int size = 2000;

    // 记录该块在物理文件中的起始偏移量
    private long offsets[] = new long[size];
    // 记录该块中第一个消息的起始消息编号
    private int queueIndexes[] = new int[size];

    private static final byte FILL_BYTE = (byte)0;

//    private List<Block> blocks = new ArrayList<>();
//    private volatile Block currentBlock;

    private long offset;
    private int queueIndex;

    /**
     * 队列的总块数
     */
    private int blockSize = 0;
    /**
     * 队列的总消息数
     */
    private int queueLength = 0;


    /**
     * put 由评测程序保证了 queue 级别的同步
     *
     * @param message
     */
    public void put(byte[] message) {
        if(firstPut){
            this.queueIndex=0;
            firstPut = false;
        }
        // 缓冲区满，先落盘
        if (SINGLE_MESSAGE_SIZE > writeBuffer.remaining()) {
            // 落盘
            flush();
        }
        if(message.length<SINGLE_MESSAGE_SIZE){
            byte[] newMessage = new byte[SINGLE_MESSAGE_SIZE];
            for(int i=0;i<SINGLE_MESSAGE_SIZE;i++){
                if(i<message.length){
                    newMessage[i] = message[i];
                }else{
                    newMessage[i] = FILL_BYTE;
                }
            }
            message = newMessage;
        }
        writeBuffer.put(message);
        this.queueLength ++;
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


        offsets[blockSize] = this.offset;
        queueIndexes[blockSize] = this.queueIndex;
        blockSize++;
        if(blockSize >offsets.length * 0.7){
            offsets = copyOf(offsets, offsets.length*2);
            queueIndexes = copyOf(queueIndexes, queueIndexes.length*2);
        }

        this.queueIndex += BLOCK_SIZE;
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

        offsets[blockSize] = this.offset;
        queueIndexes[blockSize] = this.queueIndex;
        blockSize++;
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
        if (offset > queueLength -1) {
            return DefaultQueueStoreImpl.EMPTY;
        }
        int startIndex = (int) offset;
        int endIndex = Math.min(startIndex + (int) num - 1, queueLength-1);
        int startBlock = startIndex / BLOCK_SIZE;
        int endBlock = endIndex / BLOCK_SIZE;

        List<byte[]> result = new ArrayList<>();
        for (int j = startBlock; j <= endBlock; j++) {
            long readOffset;
            int blockStartIndex;
            int size;
            if(j == startBlock){
                readOffset = this.offsets[j] + (startIndex % BLOCK_SIZE)*SINGLE_MESSAGE_SIZE;
                blockStartIndex = startIndex % BLOCK_SIZE;
            }else{
                readOffset = this.offsets[j];
                blockStartIndex = 0;
            }
            if(j == endBlock){
                size = endIndex % BLOCK_SIZE - blockStartIndex + 1;
            }else{
                size = BLOCK_SIZE - blockStartIndex;
            }

            ByteBuffer byteBuffer = readBufferHolder.get();
            byteBuffer.clear();
            byteBuffer.limit(size*SINGLE_MESSAGE_SIZE);
            try {
                channel.read(byteBuffer, readOffset);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byteBuffer.flip();
            for (int i = 0; i < size; i++) {
                byte[] bytes = new byte[SINGLE_MESSAGE_SIZE];
                byteBuffer.get(bytes);
                // TODO
//                bytes = truncate(bytes);
                result.add(bytes);
            }
        }
        return result;
    }

//    private int binarySearch(int index, int left, int right) {
//        while (left <= right) {//慎重截止条件，根据指针移动条件来看，这里需要将数组判断到空为止
//            int mid = left + ((right - left) >> 1);//防止溢出
//            if (this.queueIndexes[mid] <= index && index <= this.queueIndexes[mid] + this.messageSizes[mid] - 1) {//找到了
//                index = mid;
//                break;
//            } else if (index < this.queueIndexes[mid])
//                right = mid - 1;//给定值key一定在左边，并且不包括当前这个中间值
//            else
//                left = mid + 1;//给定值key一定在右边，并且不包括当前这个中间值
//        }
//
//        return index;
//    }

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

    private byte[] truncate(byte[] message){
        int realSize = 0;
        for(int i=0;i<SINGLE_MESSAGE_SIZE;i++){
            if(message[i]==FILL_BYTE){
                realSize = i;
                break;
            }
        }
        return Arrays.copyOf(message, realSize);
    }

}
