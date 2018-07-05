package io.openmessaging;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author 徐靖峰
 * Date 2018-07-04
 */
public class IndexBlock {

    // 每个indexBlock限制最多1000个索引
    public static final int cntLimit = 1000;
    // 记录索引块内的索引计数
    private AtomicInteger blockInnerCnt = new AtomicInteger(0);
    // 该 indexBlock 在索引文件中的起始偏移量
    private long indexFileStartPosition;

    public AtomicInteger getBlockInnerCnt() {
        return blockInnerCnt;
    }

    public void setBlockInnerCnt(AtomicInteger blockInnerCnt) {
        this.blockInnerCnt = blockInnerCnt;
    }

    public long getIndexFileStartPosition() {
        return indexFileStartPosition;
    }

    public void setIndexFileStartPosition(long indexFileStartPosition) {
        this.indexFileStartPosition = indexFileStartPosition;
    }
}
