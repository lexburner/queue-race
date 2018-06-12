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


    public static Collection<String> EMPTY = new ArrayList<String>();
    Map<String, List<String>> queueMap = new ConcurrentHashMap<String, List<String>>();

    public synchronized void put(String queueName, String message) {
        if (!queueMap.containsKey(queueName)) {
            queueMap.put(queueName, new ArrayList<String>());
        }
        queueMap.get(queueName).add(message);
    }
    public synchronized Collection<String> get(String queueName, long offset, long num) {
        if (!queueMap.containsKey(queueName)) {
            return EMPTY;
        }
        List<String> msgs = queueMap.get(queueName);
        return msgs.subList((int) offset, offset + num > msgs.size() ? msgs.size() : (int) (offset + num));
    }
}
