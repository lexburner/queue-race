package io.openmessaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 徐靖峰
 * Date 2018-06-29
 */
public class CommitLog {
    public static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    private final FileChannel fileChannel;

    public static final int commitLogNum = 10;
    // 单个commitLog大小
    public static final int commitLogSize = 1 * 1024 * 1024 * 1024;
    // 写入指针的位置
    public AtomicInteger wrotePosition;
    public MappedByteBuffer mappedByteBuffer;

    public CommitLog(Integer commitLogId) {
        try {
            this.fileChannel = new RandomAccessFile(new File(DefaultQueueStoreImpl.dir + commitLogId + ".data"), "rw").getChannel();  //初始化fileChannel
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, commitLogSize);//初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        wrotePosition = new AtomicInteger(0);
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();
        ByteBuffer slice = this.mappedByteBuffer.slice();
        if ((currentPos + data.length) <= commitLogSize) {
            try {
                slice.position(currentPos);
                slice.put(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();
        ByteBuffer slice = this.mappedByteBuffer.slice();
        if ((currentPos + length) <= commitLogSize) {
            try {
                slice.position(currentPos);
                slice.put(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    public boolean readMessage(final byte[] data, final int offset, final int length) {

        return false;
    }


}
