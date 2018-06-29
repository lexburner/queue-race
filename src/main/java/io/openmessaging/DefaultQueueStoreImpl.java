package io.openmessaging;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {


    public static final String dir = "/Users/kirito/data/";
//    public static final String dir = "/alidata1/race2018/data/";

    //存储 queue 的索引文件
    Map<String, Index> indexMap = new ConcurrentHashMap<>();
    //存储 queueName 和 queue 编号 的映射关系
    Map<String, Integer> queueNameQueueNoMap = new ConcurrentHashMap<>();
    //存储 (queueNo % CommitLog.commitLogNum) 对应的实际 commitLog
    Map<Integer, CommitLog> commitLogMap = new ConcurrentHashMap<>();
    //queue计数器
    AtomicInteger queueCnt = new AtomicInteger(0);

    ConcurrentMap<String, AtomicInteger> queueNumMap = new ConcurrentHashMap<>();

    public static Collection<byte[]> EMPTY = new ArrayList<>();

    /**
     * 把一条消息写入一个队列；
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行put；
     * 每个queue中的内容，按发送顺序存储消息（可以理解为Java中的List），同时每个消息会有一个索引，索引从0开始；
     * 不同queue中的内容，相互独立，互不影响；
     *
     * @param queueName 代表queue名字，如果是第一次put，则自动生产一个queue
     * @param message   message，代表消息的内容，评测时内容会随机产生，大部分长度在64字节左右，会有少量消息在1k左右
     */
    @Override
    public synchronized void put(String queueName, byte[] message) {
        Integer queueNo = queueNameQueueNoMap.get(queueName);
        if (queueNo==null) {
            queueNo = queueCnt.incrementAndGet();
            // 新建index
            indexMap.put(queueName, new Index(queueName));
            // 存放queueName和queueNo的关联
            queueNameQueueNoMap.put(queueName, queueNo);
        }
        Integer commitLogId = queueNo % CommitLog.commitLogNum;
        CommitLog commitLog = commitLogMap.get(commitLogId);
        if (commitLog == null) {
            CommitLog newCommitLog = new CommitLog(commitLogId);
            //新建 commitLog
            commitLogMap.put(commitLogId, newCommitLog);
            commitLog = newCommitLog;
        }


        int queueNameLength = queueName.getBytes().length;
        int messageLength = message.length;

        byte[] lengthArray = new byte[2];
        lengthArray[0] = (byte) (queueNameLength & 0xFF);
        lengthArray[1] = (byte) (messageLength & 0xFF);

        ByteBuffer byteBuffer = commitLog.mappedByteBuffer.slice();
        //设置position为当前写位置
        int position = commitLog.commitLogWritePosition.get();
        byteBuffer.position(position);
        byteBuffer.put(lengthArray);
        byteBuffer.put(queueName.getBytes());
        byteBuffer.put(message);
        //移动写指针
        commitLog.commitLogWritePosition.addAndGet(queueNameLength + messageLength + 2);
        //强制刷盘，防止内存溢出
        //TODO 不要实时刷盘
        //BUG 实时刷盘会卡死
        commitLog.mappedByteBuffer.force();

        //建立索引
        Index index = indexMap.get(queueName);
        ByteBuffer slice = index.mappedByteBuffer.slice();
        slice.position(index.IndexWrotePosition.get());
        slice.putInt(position);
        index.IndexWrotePosition.addAndGet(4);
        //TODO 不要实时刷盘
        //BUG 实时刷盘会卡死
        index.mappedByteBuffer.force();
    }

    /**
     * 从一个队列中读出一批消息，读出的消息要按照发送顺序来；
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行get；
     * 返回的Collection会被并发读，但不涉及写，因此只需要是线程读安全就可以了；
     *
     * @param queueName 代表队列的名字
     * @param offset    代表消息的在这个队列中的起始消息索引
     * @param num       代表读取的消息的条数，如果消息足够，则返回num条，否则只返回已有的消息即可;没有消息了，则返回一个空的集合
     */
    @Override
    public synchronized Collection<byte[]> get(String queueName, long offset, long num) {

        List<byte[]> list = new ArrayList<>();
        if (!indexMap.containsKey(queueName)) {
            return EMPTY;
        }

        Index index = indexMap.get(queueName);

        ByteBuffer indexByteBuffer = index.mappedByteBuffer;
        int commitLogNo = queueNameQueueNoMap.get(queueName) % CommitLog.commitLogNum;
        ByteBuffer commitLog = commitLogMap.get(commitLogNo).mappedByteBuffer;

        for (long i = 0; i < num; i++) {
            indexByteBuffer.position((int) ((offset + i) * 4));
            int pos = indexByteBuffer.getInt();

            if (pos == Integer.MAX_VALUE) {
                break;
            }

            commitLog.position(pos);

            byte[] lengthArray = new byte[2];
            commitLog.get(lengthArray);

            int queueNameLength = lengthArray[0];
            int messageLength = lengthArray[1];

            byte[] queueNameBytes = new byte[queueNameLength];
            byte[] messageBytes = new byte[messageLength];
            commitLog.get(queueNameBytes);
            commitLog.get(messageBytes);
            list.add(messageBytes);
        }
        return list;
    }

}
