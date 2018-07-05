package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 徐靖峰
 * Date 2018-06-29
 */
public class IndexManager {

    public Map<String, List<IndexBlock>> indexOffsets = new ConcurrentHashMap<>();
    public List<IndexMappedByteBufferWrapper> indexMappedByteBufferWrappers = new ArrayList<>();
    private AtomicInteger blockCnt = new AtomicInteger(0);

    public AtomicInteger indexWrotePosition = new AtomicInteger(0);

    private FileChannel fileChannel;

    // 设置 300000 个 indexBlock 为一个读写单位
    public final static long segmentSize = 100000 * 8000L;

    public IndexManager() {
//        MappedByteBuffer mappedByteBuffer = null;
//        try {
//            FileChannel fileChannel = new RandomAccessFile(new File(DefaultQueueStoreImpl.dir + queueName + ".index"), "rw").getChannel();  //初始化fileChannel
//            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4 * 40000); //初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件；
//            this.mappedByteBuffer = mappedByteBuffer;
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        try {
            this.fileChannel = new RandomAccessFile(new File(DefaultQueueStoreImpl.dir + "single.index"), "rw").getChannel();  //初始化fileChannel
            MappedByteBuffer mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize);//初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件；
            IndexMappedByteBufferWrapper indexMappedByteBufferWrapper = new IndexMappedByteBufferWrapper();
            indexMappedByteBufferWrapper.setMappedByteBuffer(mappedByteBuffer);
            indexMappedByteBufferWrapper.setStartPosition(0);
            indexMappedByteBufferWrapper.setEndPosition(segmentSize - 1);
            indexMappedByteBufferWrappers.add(indexMappedByteBufferWrapper);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized IndexMappedByteBufferWrapper getIndexMappedByteBufferWrapper(long startPosition) {
        IndexMappedByteBufferWrapper lastWrapper = indexMappedByteBufferWrappers.get(indexMappedByteBufferWrappers.size() - 1);
        // mmap 不足，扩容
        if (lastWrapper.getStartPosition() < startPosition) {
            try {
                MappedByteBuffer mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, lastWrapper.getStartPosition() + segmentSize, segmentSize);//初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件；
                IndexMappedByteBufferWrapper indexMappedByteBufferWrapper = new IndexMappedByteBufferWrapper();
                indexMappedByteBufferWrapper.setMappedByteBuffer(mappedByteBuffer);
                indexMappedByteBufferWrapper.setStartPosition(lastWrapper.getStartPosition() + segmentSize);
                indexMappedByteBufferWrapper.setEndPosition(lastWrapper.getStartPosition() + segmentSize + segmentSize - 1);
                indexMappedByteBufferWrappers.add(indexMappedByteBufferWrapper);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        for (int i = 0; i < indexMappedByteBufferWrappers.size(); i++) {
            if (indexMappedByteBufferWrappers.get(i).getStartPosition() == startPosition) {
                return indexMappedByteBufferWrappers.get(i);
            }
        }
        return null;
    }

    /**
     * 设置索引
     *
     * @param queueName
     * @param position  物理数据的 offset
     */
    public void addIndex(String queueName, long position) {
        Object lock = DefaultQueueStoreImpl.queueLocks.get(queueName);
        synchronized (lock) {
            List<IndexBlock> indexBlocks = indexOffsets.computeIfAbsent(queueName, k -> new ArrayList<>());
            // 新增索引块
            if (indexBlocks.size() == 0) {
                IndexBlock indexBlock = new IndexBlock();
                int startPosition = blockCnt.getAndIncrement();
                indexBlock.setIndexFileStartPosition(startPosition * 8000);
                indexBlocks.add(indexBlock);
            }
            // 写满一个块需要增加索引块
            IndexBlock lastBlock = indexBlocks.get(indexBlocks.size() - 1);
            if (lastBlock.getBlockInnerCnt().get() >= IndexBlock.cntLimit) {
                IndexBlock indexBlock = new IndexBlock();
                int startPosition = blockCnt.getAndIncrement();
                indexBlock.setIndexFileStartPosition(startPosition * 8000);
                indexBlocks.add(indexBlock);
            }

            IndexBlock currentIndexBlock = indexBlocks.get(indexBlocks.size() - 1);
            int blockInnerCnt = currentIndexBlock.getBlockInnerCnt().getAndIncrement();
            long indexFileStartPosition = currentIndexBlock.getIndexFileStartPosition();
            IndexMappedByteBufferWrapper indexMappedByteBufferWrapper = getIndexMappedByteBufferWrapper(indexFileStartPosition);
            ByteBuffer indexWriter = indexMappedByteBufferWrapper.getMappedByteBuffer().slice();
            indexWriter.position(blockInnerCnt * 8);
            indexWriter.putLong(position);
        }
    }

    public List<Long> getIndex(String queueName, long offset, long num) {
        List<Long> dataOffsets = new ArrayList<>();
        List<IndexBlock> indexBlocks = indexOffsets.get(queueName);
        if (indexBlocks == null || indexBlocks.size() == 0) {
            return dataOffsets;
        }
        IndexBlock indexBlock = indexBlocks.get((int) (offset / 1000));
        long startPosition = indexBlock.getIndexFileStartPosition();
        IndexMappedByteBufferWrapper indexMappedByteBufferWrapper = getIndexMappedByteBufferWrapper(startPosition);
        ByteBuffer slice = indexMappedByteBufferWrapper.getMappedByteBuffer().slice();
        // 定位到索引文件中索引的首地址
        int innerOffset = (int) (offset % 1000);
        slice.position(innerOffset * 8);
        for (int i = 0; i < num; i++) {
            if (innerOffset + i < indexBlock.getBlockInnerCnt().get()) {
                dataOffsets.add(slice.getLong());
            }
        }
        // 处理跨块的索引
        if ((offset + num - 1) / 1000 > (offset / 1000) && (offset + num - 1) / 1000 <= indexBlocks.size()) {
            IndexBlock indexBlock2 = indexBlocks.get((int) (offset + num - 1) / 1000);
            long startPosition2 = indexBlock2.getIndexFileStartPosition();
            IndexMappedByteBufferWrapper indexMappedByteBufferWrapper2 = getIndexMappedByteBufferWrapper(startPosition2);
            ByteBuffer slice2 = indexMappedByteBufferWrapper2.getMappedByteBuffer().slice();
            // 定位到索引文件中索引的首地址
            int innerOffset2 = (int) ((offset + num - 1) % 1000);
            slice2.position(0);
            for (int i = 0; i < innerOffset2; i++) {
                if (i < indexBlock.getBlockInnerCnt().get()) {
                    dataOffsets.add(slice2.getLong());
                }
            }
        }
        return dataOffsets;
    }


}
