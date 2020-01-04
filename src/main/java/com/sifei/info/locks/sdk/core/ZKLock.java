package com.sifei.info.locks.sdk.core;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKLock implements ZKNotify {

    private ZKService service;
    // 连接地址
    private String address;
    // 目录前缀
    private final String prefix = "/locks";
    // countDownLatch 最大时长
    private int MAX_WAIT = 30000;
    // 超时阻塞
    private CountDownLatch latch;
    // 等待通知获取锁的节点
    private String waitNode;
    // 本例获得的锁节点
    private String lockNode;
    // 最后一个节点锁
    private String lastNode;

    public ZKLock(String address) throws IOException {
        if (address.isEmpty()) {
            throw new NullPointerException();
        }
        this.address = address;
        initService();
    }

    private void initService() throws IOException {
        if (this.service == null) {
            service = new ZKService(this);
        }
    }

    // 尝试获取分布式锁
    public void acquireDistributedLock(){
        try {
            if (this.tryLock()) {
                return;
            }else {
                waitForLock(waitNode, MAX_WAIT);
            }
        } catch (KeeperException e) {
            throw new ZKLockException(e);
        } catch (InterruptedException e) {
            throw new ZKLockException(e);
        }
    }

    // 释放分布式锁
    public void releaseDistributedLock(){
        try {
            boolean isWatcher = false;
            if (lockNode.equals(lastNode)){
                isWatcher = true;
            }
            service.deleteNode(lockNode, isWatcher);
            lockNode = null;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    // 创建分布式锁
    private boolean tryLock(){
        try {
            // 第一个获取锁的对象。
            lockNode = service.createNode(prefix + "/", new byte[0]);
            List<String> locks = service.getZKChildrenNode(prefix);
            Collections.sort(locks);
            if(lockNode.equals(prefix + "/" + locks.get(0))){
                // 如果是最小的节点,则表示取得锁
                return true;
            }
            if (lockNode.equals(prefix + "/" + locks.get(locks.size() - 1))) {
                this.lastNode = lockNode;
            }
            // 不是最小节点，等待上一个节点释放锁，监听上一个节点的释放
            // 遍历节点找到比自己小1的节点
            int previousLockIndex = -1;
            for (int i = 0; i < locks.size(); i++) {
                if (lockNode.equals(prefix + "/" + locks.get(i))){
                    previousLockIndex = i-1;
                    break;
                }
            }
            // 等待的节点
            this.waitNode = locks.get(previousLockIndex);
        } catch (KeeperException e) {
            throw new ZKLockException(e);
        } catch (InterruptedException e) {
            throw new ZKLockException(e);
        }
        return false;
    }

    private boolean waitForLock(String node, long waitTime) throws InterruptedException, KeeperException{
        Stat stat = service.isExitZKPath(prefix + "/" + node);
        if (stat != null) {
            // 阻塞等待锁
            this.latch = new CountDownLatch(1);
            this.latch.await(waitTime, TimeUnit.MILLISECONDS);
            this.latch = null;
        }
        return true;
    }

    @Override
    public String getAddress() {
        return this.address;
    }

    @Override
    public void successConnNotify() {

    }

    @Override
    public void nodeDeleteNotify() {
        this.latch.countDown();
    }

    public class ZKLockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public ZKLockException(String e) {
            super(e);
        }
        public ZKLockException(Exception e) {
            super(e);
        }
    }
}
