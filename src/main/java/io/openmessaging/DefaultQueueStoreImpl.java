package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    private final int fileSize = 512 * 1024 * 1024;


//    public static final String dir = "/Users/kirito/data/";
    public static final String dir = "/alidata1/race2018/data/";


    //仅允许依赖JavaSE 8 包含的lib
    Map<String, MappedByteBuffer> queueBuffer = new ConcurrentHashMap<String, MappedByteBuffer>();
    Map<String, MappedByteBuffer> oBuff = new ConcurrentHashMap<String, MappedByteBuffer>();
    FileChannel fileChannel;
    MappedByteBuffer mappedByteBuffer;
    AtomicInteger wrotePosition = new AtomicInteger(0);
    AtomicInteger wrotePositionabc = new AtomicInteger(0);

    ConcurrentMap<String, AtomicInteger> queueNumMap = new ConcurrentHashMap<>();

    AtomicInteger wrotePosition1 = new AtomicInteger(0);

    BlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<ByteBuffer>();

    {

        try {
            this.fileChannel = new RandomAccessFile(new File(dir+"lrz"), "rw").getChannel();  //初始化fileChannel
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize); //初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件；
        } catch (IOException e) {
            e.printStackTrace();
        }
        // getMmap();

        new Thread(() -> {
           while (true) { //没有停止
                try {
                    runjob();
                   // main1(null);
                    Thread.sleep(25);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    private MappedByteBuffer getMmap(String name) {
        MappedByteBuffer mappedByteBuffer = null;
        try {
            FileChannel fileChannel = new RandomAccessFile(new File(dir + name), "rw").getChannel();  //初始化fileChannel
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4 * 30000); //初始化mappedByteBuffer  对得到的缓冲区的更改最终将写入文件；
            ByteBuffer byteBuffer = mappedByteBuffer.slice();
            for (int i = 0; i < 30000; i++) {
                byteBuffer.position(i*4);
                byteBuffer.putInt(Integer.MAX_VALUE);
            }
        } catch (IOException e) {
//            e.printStackTrace();
        }
        return mappedByteBuffer;
    }


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
        if (!queueBuffer.containsKey(queueName)) {
            queueBuffer.put(queueName, getMmap(queueName));
        }

        int len = queueName.getBytes().length;
        int len1 = message.length;


        if(wrotePosition.get()+(len+len1+2)>fileSize){
            //新建文件
            System.out.println("===========================");
        }

        byte[] lengs = new byte[2];
        lengs[0] = (byte) (len & 0xFF);
        lengs[1] = (byte) (len1 & 0xFF);

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();//ByteBuffer.allocateDirect(len+len1+2);
        byteBuffer.position(wrotePosition.get()); //设置position为当前写位置
        //System.out.println(wrotePosition.get());
        byteBuffer.put(lengs);
        byteBuffer.put(queueName.getBytes());
        byteBuffer.put(message);
        this.wrotePosition.addAndGet(len + len1 + 2);//原子操作
        //System.out.println(wrotePositionabc.getAndIncrement());
        /*if(wrotePositionabc.get()>299996){

        }*/



    }

    private ByteBuffer getByteBuff() {
        return this.mappedByteBuffer.slice();
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
        if (!queueBuffer.containsKey(queueName)) {
            return EMPTY;
        }


        ByteBuffer abc = queueBuffer.get(queueName).slice();//ByteBuffer.allocateDirect(len+len1+2);
        ByteBuffer byteBuffer = getByteBuff();//ByteBuffer.allocateDirect(len+len1+2);

        for (long i = 0; i < num  ; i++) {
            abc.position((int) ((offset + i)*4));
            int pos = abc.getInt();

            if(pos==Integer.MAX_VALUE){
                break;
            }


            byteBuffer.position(pos);
            byte[] lengs = new byte[2];

            byteBuffer.get(lengs);
            int len = lengs[0];
            int len1 = lengs[1];

            byte[] lenga = new byte[len];
            byte[] lenga2 = new byte[len1];
            byteBuffer.get(lenga);
            byteBuffer.get(lenga2);
            list.add(lenga2);
        }
        return list;
    }

    public  void runjob() {

        ByteBuffer byteBuffer = getByteBuff();//ByteBuffer.allocateDirect(len+len1+2);
        byteBuffer.position(wrotePosition1.get());
        // byteBuffer.clear();

        while(true) {
            //System.out.println(len);
            byte[] lengs = new byte[2];

            byteBuffer.get(lengs);
            int len = lengs[0];
            int len1 = lengs[1];

            if (len == 0 || len1==0) {
                break;
            }
            byte[] lenga = new byte[len];
            byte[] lenga2 = new byte[len1];
            ////System.out.println(len);
            byteBuffer.get(lenga);
            byteBuffer.get(lenga2);

            String queueName = new String(lenga);
            byte[] msg = lenga2;
           // //System.out.println(queueName+"----------->"+msg);
           // printWriter.println(queueName+"----------->"+msg);

            //if(queueName!=null && queueName.)

            if((len+len1) != (queueName.length()+msg.length)){
                break;
            }

            try {
                if (!queueBuffer.containsKey(queueName)) {
                   // System.out.println((len+len1) +"--------->"+ (queueName.length()+msg.length()));
                  //  System.out.println(queueName+"----------->"+msg);
                    queueBuffer.put(queueName, getMmap(queueName));
                }

                if (!queueNumMap.containsKey(queueName)) {
                    // System.out.println((len+len1) +"--------->"+ (queueName.length()+msg.length()));
                    //  System.out.println(queueName+"----------->"+msg);
                    queueNumMap.put(queueName, new AtomicInteger(0));
                }
            } catch (Exception e) {
               break;
            }

            ByteBuffer abc = queueBuffer.get(queueName).slice();//ByteBuffer.allocateDirect(len+len1+2);
            abc.position(queueNumMap.get(queueName).get()); //设置position为当前写位置
            abc.putInt(wrotePosition1.get());
            wrotePosition1.addAndGet(len + len1 + 2);//原子操作
            queueNumMap.get(queueName).addAndGet(4);



        }
    }



    public static void main(String[] args) {
        //main1(args);

        DefaultQueueStoreImpl defaultQueueStoreImpl = new DefaultQueueStoreImpl();
        for (int i = 0; i < 1000; i++) {
           Collection<byte[]> msgs = defaultQueueStoreImpl.get("Queue-"+i, 16, 10);
           //Collection<String> msgs = defaultQueueStoreImpl.get("Queue-396", 0, 40);
            System.out.println("Queue-"+i);
            for (byte[] str : msgs) {
                System.out.println(new String(str));
            }
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
