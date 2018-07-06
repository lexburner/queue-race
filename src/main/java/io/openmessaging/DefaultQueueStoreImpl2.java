package io.openmessaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultQueueStoreImpl2 extends QueueStore {


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