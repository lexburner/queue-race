package io.openmessaging;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//这是评测程序的一个demo版本，其评测逻辑与实际评测程序基本类似，但是比实际评测简单很多
//该评测程序主要便于选手在本地优化和调试自己的程序

public class DemoTester {

    public static void main(String args[]) throws Exception {
        //评测相关配置
        //发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
        int msgNum  = 10000000;
        //发送阶段的最大持续时间，也即在该时间内，如果消息依然没有发送完毕，则退出评测
        int sendTime = 10 * 60 * 1000;
        //消费阶段的最大持续时间，也即在该时间内，如果消息依然没有消费完毕，则退出评测
        int checkTime = 10 * 60 * 1000;
        //队列的数量
        int queueNum = 1000;
        //正确性检测的次数
        int checkNum = 1000;
        //消费阶段的总队列数量
        int checkQueueNum = 100;
        //发送的线程数量
        int sendTsNum = 10;
        //消费的线程数量
        int checkTsNum = 10;

        ConcurrentMap<String, AtomicInteger> queueNumMap = new ConcurrentHashMap<>();
        for (int i = 0; i < queueNum; i++) {
            queueNumMap.put("Queue-" + i, new AtomicInteger(0));
        }

        QueueStore queueStore = null;

        try {
            Class queueStoreClass = Class.forName("io.openmessaging.DefaultQueueStoreImpl");
            queueStore = (QueueStore)queueStoreClass.newInstance();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(-1);
        }

        //Step1: 发送消息
        long sendStart = System.currentTimeMillis();
        long maxTimeStamp = System.currentTimeMillis() + sendTime;
        AtomicLong sendCounter = new AtomicLong(0);
        Thread[] sends = new Thread[sendTsNum];
        for (int i = 0; i < sendTsNum; i++) {
            sends[i] = new Thread(new Producer(queueStore, i, maxTimeStamp, msgNum, sendCounter, queueNumMap));
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].start();
        }
        for (int i = 0; i < sendTsNum; i++) {
            sends[i].join();
        }
        long sendSend = System.currentTimeMillis();
        System.out.printf("Send: %d ms Num:%d\n", sendSend - sendStart, sendCounter.get());
        long maxCheckTime = System.currentTimeMillis() + checkTime;

        //Step2: 索引的正确性校验
        long indexCheckStart = System.currentTimeMillis();
        AtomicLong indexCheckCounter = new AtomicLong(0);
        Thread[] indexChecks = new Thread[checkTsNum];
        for (int i = 0; i < sendTsNum; i++) {
            indexChecks[i] = new Thread(new IndexChecker(queueStore, i, maxCheckTime, checkNum, indexCheckCounter, queueNumMap));
        }
        for (int i = 0; i < sendTsNum; i++) {
            indexChecks[i].start();
        }
        for (int i = 0; i < sendTsNum; i++) {
            indexChecks[i].join();
        }
        long indexCheckEnd = System.currentTimeMillis();
        System.out.printf("Index Check: %d ms Num:%d\n", indexCheckEnd - indexCheckStart, indexCheckCounter.get());

        //Step3: 消费消息，并验证顺序性
        long checkStart = System.currentTimeMillis();
        Random random = new Random();
        AtomicLong checkCounter = new AtomicLong(0);
        Thread[] checks = new Thread[checkTsNum];
        for (int i = 0; i < sendTsNum; i++) {
            int eachCheckQueueNum = checkQueueNum/checkTsNum;
            ConcurrentMap<String, AtomicInteger> offsets = new ConcurrentHashMap<>();
            for (int j = 0; j < eachCheckQueueNum; j++) {
                String queueName = "Queue-" + random.nextInt(queueNum);
                while (offsets.containsKey(queueName)) {
                    queueName = "Queue-" + random.nextInt(queueNum);
                }
                offsets.put(queueName, queueNumMap.get(queueName));
            }
            checks[i] = new Thread(new Consumer(queueStore, i, maxCheckTime, checkCounter, offsets));
        }
        for (int i = 0; i < sendTsNum; i++) {
            checks[i].start();
        }
        for (int i = 0; i < sendTsNum; i++) {
            checks[i].join();
        }
        long checkEnd = System.currentTimeMillis();
        System.out.printf("Check: %d ms Num: %d\n", checkEnd - checkStart, checkCounter.get());

        //评测结果
        System.out.printf("Tps:%f\n", ((sendCounter.get() + checkCounter.get() + indexCheckCounter.get()) + 0.1) * 1000 / ((sendSend- sendStart) + (checkEnd- checkStart) + (indexCheckEnd - indexCheckStart)));
    }
    static class Producer implements Runnable {

        private AtomicLong counter;
        private ConcurrentMap<String, AtomicInteger> queueCounter;
        private long maxMsgNum;
        private QueueStore queueStore;
        private int number;
        private long maxTimeStamp;
        public Producer(QueueStore queueStore, int number, long maxTimeStamp, int maxMsgNum, AtomicLong counter, ConcurrentMap<String, AtomicInteger> queueCounter) {
            this.counter = counter;
            this.maxMsgNum = maxMsgNum;
            this.queueCounter = queueCounter;
            this.number = number;
            this.queueStore = queueStore;
            this.maxTimeStamp =  maxTimeStamp;
        }

        @Override
        public void run() {
            long count;
            while ( (count = counter.getAndIncrement()) < maxMsgNum && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    String queueName = "Queue-" + count % queueCounter.size();
                    synchronized (queueCounter.get(queueName)) {
                        queueStore.put(queueName, String.valueOf(queueCounter.get(queueName).getAndIncrement()).getBytes());
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    static class IndexChecker implements Runnable {

        private AtomicLong counter;
        private long maxMsgNum;
        private QueueStore queueStore;
        private long maxTimeStamp;
        private int number;
        private ConcurrentMap<String, AtomicInteger> queueCounter;
        public IndexChecker(QueueStore queueStore, int number, long maxTimeStamp, int maxMsgNum, AtomicLong counter, ConcurrentMap<String, AtomicInteger> queueCounter) {
            this.counter = counter;
            this.maxMsgNum = maxMsgNum;
            this.queueStore = queueStore;
            this.number = number;
            this.queueCounter = queueCounter;
            this.maxTimeStamp =  maxTimeStamp;
        }

        @Override
        public void run() {
            Random random = new Random();
            while (counter.getAndIncrement() < maxMsgNum && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    String queueName = "Queue-" + random.nextInt(queueCounter.size());
                    int index = random.nextInt(queueCounter.get(queueName).get()) - 10;
                    if (index < 0) index = 0;
                    Collection<byte[]> msgs = queueStore.get(queueName, index, 10);
                    for (byte[] msg : msgs) {
                        if (!new String(msg).equals(String.valueOf(index++))) {
                            System.out.println("Check error");
                            System.exit(-1);
                        }
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);

                }
            }
        }
    }

    static class Consumer implements Runnable {

        private AtomicLong counter;
        private QueueStore queueStore;
        private ConcurrentMap<String, AtomicInteger> offsets;
        private long maxTimeStamp;
        private int number;
        public Consumer(QueueStore queueStore, int number, long maxTimeStamp, AtomicLong counter, ConcurrentMap<String, AtomicInteger> offsets) {
            this.counter = counter;
            this.queueStore = queueStore;
            this.offsets = offsets;
            this.maxTimeStamp = maxTimeStamp;
            this.number =  number;
        }

        @Override
        public void run() {
            ConcurrentMap<String, AtomicInteger> pullOffsets = new ConcurrentHashMap<>();
            for (String queueName: offsets.keySet()) {
                pullOffsets.put(queueName, new AtomicInteger(0));
            }
            while (pullOffsets.size() > 0 && System.currentTimeMillis() <= maxTimeStamp) {
                try {
                    for (String queueName : pullOffsets.keySet()) {
                        int index = pullOffsets.get(queueName).get();
                        Collection<byte[]> msgs = queueStore.get(queueName, index, 10);
                        if (msgs != null && msgs.size() > 0) {
                            pullOffsets.get(queueName).getAndAdd(msgs.size());
                            for (byte[] msg : msgs) {
                                if (!new String(msg).equals(String.valueOf(index++))) {
                                    System.out.println("Check error");
                                    System.exit(-1);
                                }
                            }

                            counter.addAndGet(msgs.size());
                        }
                        if (msgs == null || msgs.size() < 10) {
                            if (pullOffsets.get(queueName).get() != offsets.get(queueName).get()) {
                                System.out.printf("Queue Number Error");
                                System.exit(-1);
                            }
                            pullOffsets.remove(queueName);
                        }
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }
}


