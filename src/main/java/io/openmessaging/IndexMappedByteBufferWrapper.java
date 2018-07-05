package io.openmessaging;

import java.nio.MappedByteBuffer;

/**
 * @author 徐靖峰
 * Date 2018-07-04
 */
public class IndexMappedByteBufferWrapper {

    private MappedByteBuffer mappedByteBuffer;
    private long startPosition;
    private long endPosition;

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public void setMappedByteBuffer(MappedByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(long startPosition) {
        this.startPosition = startPosition;
    }

    public long getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(long endPosition) {
        this.endPosition = endPosition;
    }
}
