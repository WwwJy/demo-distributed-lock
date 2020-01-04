package com.sifei.info.locks.sdk.core;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZKService {

    private ZooKeeper zk;

    public ZKService(ZKNotify notify) throws IOException {
        ZKConnection conn = new ZKConnection();
        conn.setNotify(notify);
        zk = conn.connection(notify.getAddress());
    }

    /**
     * 创建ZK节点
     * @param path
     * @param data
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected String createNode(String path, byte[] data) throws KeeperException, InterruptedException {
        //访问控制列表
        ArrayList<ACL> openAclUnsafe = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        //创建模式
        CreateMode mode = CreateMode.EPHEMERAL_SEQUENTIAL;
        String result = zk.create(path, data, openAclUnsafe, mode);
        return result;
    }

    /**
     * 删除ZK节点
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected void deleteNode(String path, boolean isWatcher) throws KeeperException, InterruptedException {
        zk.exists(path,isWatcher);
        zk.delete(path,-1);
    }

    /**
     * 获取ZK节点
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected String getZKNodeData(String path) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(path, false, new Stat());
        return new String(data);
    }

    /**
     * 获取某个节点下面的所有子节点
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected List<String> getZKChildrenNode(String path) throws KeeperException, InterruptedException {
        List<String> nodes = zk.getChildren(path,false);
        return nodes;
    }
    /**
     * 设置zk节点数据信息
     * @param path
     * @param data
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected Stat setZKNodeData(String path, byte[] data) throws KeeperException, InterruptedException {
        zk.exists(path,true);
        return zk.setData(path, data, -1);
    }

    /**
     * 判断节点是否存在
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected Stat isExitZKPath(String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, true);
        return stat;
    }
}
