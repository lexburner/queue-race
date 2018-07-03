package io.openmessaging;

import java.nio.MappedByteBuffer;

/**
 * @author 徐靖峰
 * Date 2018-07-02
 */
public class Block {
    private MappedByteBuffer mappedByteBuffer;
    private int startPosition;
    private int endPosition;

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public void setMappedByteBuffer(MappedByteBuffer mappedByteBuffer) {
        this.mappedByteBuffer = mappedByteBuffer;
    }

    public int getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(int startPosition) {
        this.startPosition = startPosition;
    }

    public int getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(int endPosition) {
        this.endPosition = endPosition;
    }
}
