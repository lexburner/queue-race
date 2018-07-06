package io.openmessaging;


/**
 * @author 徐靖峰
 * Date 2018-07-02
 */
public class Block {

    /**
     * messageSize 随 block 写入递增
     * messageLength 随 block 写入递增
     * offset 落盘时确定
     * queueIndex 初始化时设置
     */

    // 记录该块有多少个消息
    public int messageSize = 0;
    // 消息具体的长度
    public int messageLength = 0;
    // 记录该块在物理文件中的起始偏移量
    public long offset;
    // 记录该块中第一个消息的起始消息编号
    public int queueIndex;

}
