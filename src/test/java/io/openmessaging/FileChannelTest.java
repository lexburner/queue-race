package io.openmessaging;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-07-09
 */
public class FileChannelTest {

    private final static String dir = "/Users/kirito/data/";

    public static void main(String[] args) throws IOException, InterruptedException {
        testSingleThread(2);
        testSingleThread(4);
        testSingleThread(8);
        testSingleThread(16);
        testMultiThread(10, 2);
        testMultiThread(10, 4);
        testMultiThread(10, 8);
        testMultiThread(10, 16);

    }

    static void testSingleThread(int n) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(dir + "test" + n + "k.data", "rw");
        FileChannel channel = randomAccessFile.getChannel();
        int singleMessageSize = 58;
        byte[] message = new byte[singleMessageSize];
        Arrays.fill(message, (byte) 'a');
        // 写入 10g 数据
        long fileSize = 10L * 1024 * 1024 * 1024;
        ByteBuffer writeBuffer = ByteBuffer.allocateDirect(n * 1024);
        long start = System.currentTimeMillis();
        for (long i = 0; i < fileSize / (n * 1024); i++) {
            writeBuffer.clear();
            while (writeBuffer.remaining() >= singleMessageSize) {
                writeBuffer.put(message);
            }
            writeBuffer.flip();
            channel.write(writeBuffer);
        }
        System.out.println("单线程顺序写入 10g，每次写入 " + n + "k 字节，耗时 " + (System.currentTimeMillis() - start) + " ms");
        randomAccessFile.close();
    }

    static void testMultiThread(int nThread, int n) throws IOException, InterruptedException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(dir + "testConcurrent" + n + ".data", "rw");
        FileChannel channel = randomAccessFile.getChannel();
        int singleMessageSize = 58;
        byte[] message = new byte[singleMessageSize];
        Arrays.fill(message, (byte) 'a');
        ExecutorService executorService = Executors.newFixedThreadPool(nThread);
        AtomicLong wrotePosition = new AtomicLong(0);
        // 写入 10g 数据
        long fileSize = 10L * 1024 * 1024 * 1024;
        ThreadLocal<ByteBuffer> writeBufferHolder = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(n * 1024));
        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch((int) (fileSize / (n * 1024)));
        for (long i = 0; i < fileSize / (n * 1024); i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    long position = wrotePosition.getAndAdd(n * 1024);
                    ByteBuffer writeBuffer = writeBufferHolder.get();
                    writeBuffer.clear();
                    while (writeBuffer.remaining() >= singleMessageSize) {
                        writeBuffer.put(message);
                    }
                    writeBuffer.flip();
                    try {
                        channel.write(writeBuffer, position);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        executorService.shutdown();
        System.out.println(nThread + "线程随机写入 10g，每次写入 " + n + "k 字节，耗时 " + (System.currentTimeMillis() - start) + " ms");
        randomAccessFile.close();
    }
}
