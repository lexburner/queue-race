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

    public final static int SINGLE_MESSAGE_SIZE = 58;
    public final static int BLOCK_SIZE = 40;

    private FileChannel channel;
    private AtomicLong wrotePosition;

    private volatile boolean firstGet = true;
    private volatile boolean firstPut = true;

    public Queue(FileChannel channel, AtomicLong wrotePosition) {
        this.channel = channel;
        this.wrotePosition = wrotePosition;
    }

    // 缓冲区大小
    public final static int bufferSize = SINGLE_MESSAGE_SIZE * BLOCK_SIZE;

    // 读写缓冲区
    private ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
    //
    private int lastReadOffset = -1;

    private static final int size = 2000 / BLOCK_SIZE + 1;
//    private static final int size = 2000;

    // 记录该块在物理文件中的起始偏移量
    private long offsets[] = new long[size];
    // 记录该块中第一个消息的起始消息编号
    private int queueIndexes[] = new int[size];

    private static final byte FILL_BYTE = (byte)0;

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
        if (SINGLE_MESSAGE_SIZE > buffer.remaining()) {
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
        buffer.put(message);
        this.queueLength ++;
    }

    private void flush() {
        buffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        this.offset = writePosition;
        try {
            channel.write(buffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.clear();


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
        buffer.flip();
        long writePosition = wrotePosition.getAndAdd(bufferSize);
        this.offset = writePosition;
        try {
            channel.write(buffer, writePosition);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.clear();
//        ((DirectBuffer) buffer).cleaner().clean();

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
    public synchronized Collection<byte[]> get(long offset, long num) {
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

        List<byte[]> result = new ArrayList<>();

        if(lastReadOffset==startIndex){
            while (startIndex<=endIndex&&buffer.hasRemaining()){
                startIndex++;
                byte[] cacheMessage = new byte[SINGLE_MESSAGE_SIZE];
                buffer.get(cacheMessage);
                // todo
//                cacheMessage = truncate(cacheMessage);
                result.add(cacheMessage);
                lastReadOffset++;
            }
        }
        // 从 cache 中获取到了所有的消息
        if(startIndex>endIndex){
            return result;
        }

        int startBlock = startIndex / BLOCK_SIZE;
        int endBlock = endIndex / BLOCK_SIZE;

        for (int j = startBlock; j <= endBlock; j++) {
//            long readOffset;
//            int blockStartIndex;
//            int size;
//            if(j == startBlock){
//                readOffset = this.offsets[j] + (startIndex % BLOCK_SIZE)*SINGLE_MESSAGE_SIZE;
//                blockStartIndex = startIndex % BLOCK_SIZE;
//            }else{
//                readOffset = this.offsets[j];
//                blockStartIndex = 0;
//            }
//            if(j == endBlock){
//                size = endIndex % BLOCK_SIZE - blockStartIndex + 1;
//            }else{
//                size = BLOCK_SIZE - blockStartIndex;
//            }
            int blockStartIndex = j *BLOCK_SIZE;


            buffer.clear();
            try {
                channel.read(buffer, this.offsets[j]);
            } catch (IOException e) {
                e.printStackTrace();
            }
            buffer.flip();
            for (int i = 0; i < BLOCK_SIZE; i++) {
                if(startIndex <= blockStartIndex+i && blockStartIndex+i <= endIndex){
                    byte[] bytes = new byte[SINGLE_MESSAGE_SIZE];
                    buffer.get(bytes);
                    // TODO
//                    bytes = truncate(bytes);
                    result.add(bytes);
                    this.lastReadOffset = blockStartIndex+i+1;
                }else if(blockStartIndex + i > endIndex){
                    break;
                }else {
                    // skip
                    byte[] bytes = new byte[SINGLE_MESSAGE_SIZE];
                    buffer.get(bytes);
                }

            }
        }
        return result;
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
