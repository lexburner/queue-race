package io.openmessaging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 徐靖峰
 * Date 2018-06-29
 */
public class CommitLog {

    private final FileChannel fileChannel;


    public static final int commitLogNum = 20;
    // 单个commitLog大小
    public static final int commitLogSize = 1 * 1024 * 1024 * 1024;
    // 写入指针的位置
    public AtomicInteger commitLogWritePosition;
    public MappedByteBuffer mappedByteBuffer;

    public CommitLog(Integer commitLogId) {
        try {
            this.fileChannel = new RandomAccessFile(new File(DefaultQueueStoreImpl.dir + commitLogId + ".data"), "rw").getChannel();  //初始化fileChannel
            MappedByteBuffer mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, commitLogSize);//初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件
            this.mappedByteBuffer = mappedByteBuffer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        commitLogWritePosition = new AtomicInteger(0);

    }
}
