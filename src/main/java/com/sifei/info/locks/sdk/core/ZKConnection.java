package com.sifei.info.locks.sdk.core;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZKConnection implements Watcher {

    private ZKNotify notify;
    /**
     * 连接zookeeper代理服务
     * @param address
     * @return
     * @throws IOException
     */
    public ZooKeeper connection(final String address) throws IOException {
        ZooKeeper zk = new ZooKeeper(address, 50 * 1000, this);
        return zk;
    }

    public void setNotify(ZKNotify notify) {
        this.notify = notify;
    }

    @Override
    public void process(WatchedEvent event) {
        // 节点名称
        String path = event.getPath();
        // 事件类型
        Event.EventType type = event.getType();
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == type) {
                // 成功连接上zookeeper 服务器
                System.out.println("ZK服务连接成功");
            }
            else if (Event.EventType.NodeCreated == type) {
                // 成功创建节点信息
                System.out.println("成功创建ZK节点: " + path);
            }
            else if (Event.EventType.NodeDataChanged == type) {
                // 修改节点数据
                System.out.println("成功更新ZK节点数据: " + path);
            }
            else if (Event.EventType.NodeChildrenChanged == type) {
                // 修改子节点数据
                System.out.println("成功更新ZK子节点数据: " + path);
            }
            else if (Event.EventType.NodeDeleted == type){
                // 删除节点事件
                System.out.println("成功删除ZK节点: " + path);
                if (notify != null) {
                    notify.nodeDeleteNotify();
                }
            }
        }
        else if (Event.KeeperState.Disconnected == event.getState()) {
            System.out.println("与ZK服务器断开连接");
        }
        else if (Event.KeeperState.AuthFailed == event.getState()) {
            System.out.println("ZK权限检查失败");
        }
        else if (Event.KeeperState.Expired == event.getState()) {
            System.out.println("ZK会话失效");
        }
    }
}
