package io.openmessaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-06-29
 */
public class CommitLog {
    public static final Logger log = LoggerFactory.getLogger(CommitLog.class);

    private final FileChannel fileChannel;

    public static final int commitLogNum = 20;

    // commitLog 单个段的大小
    public static final long segmentSize = (long) (1.5 * 1024 * 1024 * 1024);
    //    public static final int segmentSize = (int) (10 * 1024 * 1024);
    // 当前 commitLog 段的数量
    public int segmentNum;
    // 写入指针的位置
    public AtomicLong wrotePosition;
    List<Block> blocks = new ArrayList<>();

    public CommitLog(Integer commitLogId) {
        try {
            this.fileChannel = new RandomAccessFile(new File(DefaultQueueStoreImpl.dir + commitLogId + ".data"), "rw").getChannel();  //初始化fileChannel
            //初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件
            Block block = new Block();
            block.setMappedByteBuffer(this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize));
            block.setStartPosition(0);
            block.setEndPosition(segmentSize);
            blocks.add(block);
            segmentNum = 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        wrotePosition = new AtomicLong(0);
    }

    public void appendMessage(final byte[] data) {
        long currentPos = this.wrotePosition.get();
        if ((this.wrotePosition.get() + data.length) > getCurrentBlock().getEndPosition()) {
            try {
                // 获取上一段
                Block oldBlock = blocks.get(segmentNum - 1);
                oldBlock.setEndPosition(currentPos - 1);
                // 设置新段
                Block newBlock = new Block();
                newBlock.setStartPosition(currentPos);
                newBlock.setEndPosition(currentPos + segmentSize);
                MappedByteBuffer newMappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, currentPos, currentPos + segmentSize);
                newBlock.setMappedByteBuffer(newMappedByteBuffer);
                blocks.add(newBlock);
                // 设置新的写buffer
                // 增加 segment,移动block
                segmentNum++;
            } catch (Exception e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
        }
        ByteBuffer slice = getCurrentBlock().getMappedByteBuffer().slice();
        slice.position((int) (currentPos - getCurrentBlock().getStartPosition()));
        slice.put(ByteBuffer.wrap(data));
        this.wrotePosition.addAndGet(data.length);
    }

    private Block getCurrentBlock() {
        return this.blocks.get(segmentNum - 1);
    }

    public byte[] readMessage(final long position) {
        ByteBuffer slice = null;
        long startPosition = 0;
        for (Block block : blocks) {
            if (block.getStartPosition() <= position && block.getEndPosition() > position) {
                slice = block.getMappedByteBuffer().slice();
                startPosition = block.getStartPosition();
                break;
            }
        }
        try {
            if (slice == null) {
                return null;
            }
            slice.position((int) (position - startPosition));

            byte[] lengthArray = new byte[2];
            slice.get(lengthArray);

            int queueNameLength = lengthArray[0];
            int messageLength = lengthArray[1];

            byte[] queueNameBytes = new byte[queueNameLength];
            byte[] messageBytes = new byte[messageLength];
            slice.get(queueNameBytes);
            slice.get(messageBytes);
            return messageBytes;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


}
