package io.openmessaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {


    public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();
    Map<String, List<byte[]>> queueMap = new ConcurrentHashMap<String, List<byte[]>>();

    public synchronized void put(String queueName, byte[] message) {
        if (!queueMap.containsKey(queueName)) {
            queueMap.put(queueName, new ArrayList<byte[]>());
        }
        queueMap.get(queueName).add(message);
    }
    public synchronized Collection<byte[]> get(String queueName, long offset, long num) {
        if (!queueMap.containsKey(queueName)) {
            return EMPTY;
        }
        List<byte[]> msgs = queueMap.get(queueName);
        return msgs.subList((int) offset, offset + num > msgs.size() ? msgs.size() : (int) (offset + num));
    }
}
