package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 徐靖峰
 * Date 2018-06-29
 */
public class Index {

    public AtomicInteger IndexWrotePosition = new AtomicInteger(0);

    public MappedByteBuffer mappedByteBuffer;

    public Index(String queueName) {
        MappedByteBuffer mappedByteBuffer = null;
        try {
            FileChannel fileChannel = new RandomAccessFile(new File(DefaultQueueStoreImpl.dir + queueName +".index"), "rw").getChannel();  //初始化fileChannel
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4 * 40000); //初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件；
            ByteBuffer byteBuffer = mappedByteBuffer.slice();
            for (int i = 0; i < 40000; i++) {
                byteBuffer.position(i * 4);
                byteBuffer.putInt(Integer.MAX_VALUE);
            }
            this.mappedByteBuffer = mappedByteBuffer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
