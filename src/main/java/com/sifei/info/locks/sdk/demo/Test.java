package com.sifei.info.locks.sdk.demo;

import com.sifei.info.locks.sdk.core.ZKLock;

public class Test {

    public static void main(String[] args) {
        Demo d1 = new Demo();
        new Thread(d1).start();
        new Thread(d1).start();
        new Thread(d1).start();
        new Thread(d1).start();
    }
}

class Demo implements Runnable{

    @Override
    public void run() {
        try {
            ZKLock lock = new ZKLock("localhost:2181");
            lock.acquireDistributedLock();
            Thread.sleep(2000);
            System.out.println(System.currentTimeMillis());
            lock.releaseDistributedLock();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
