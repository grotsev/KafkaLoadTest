package kz.greetgo.hotstandby;

/**
 * Created by den on 26.05.16.
 */
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;

public class LeaderLatchExample {

    private CuratorFramework client;
    private String latchPath;
    private String id;
    private LeaderLatch leaderLatch;

    public LeaderLatchExample(String connString, String latchPath, String id) {
        client = CuratorFrameworkFactory.newClient(connString, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        this.id = id;
        this.latchPath = latchPath;
    }

    public void start() throws Exception {
        client.start();
        client.getZookeeperClient().blockUntilConnectedOrTimedOut();
        client.create().creatingParentContainersIfNeeded();
        leaderLatch = new LeaderLatch(client, latchPath, id);
        leaderLatch.start();
    }

    public boolean isLeader() {
        return leaderLatch.hasLeadership();
    }

    public Participant currentLeader() throws Exception {
        return leaderLatch.getLeader();
    }

    public void close() throws IOException {
        leaderLatch.close();
        client.close();
    }


    public static void main(String[] args) throws Exception {
        String latchPath = "/latch";
        String connStr = "localhost:2181";
        LeaderLatchExample node1 = new LeaderLatchExample(connStr, latchPath, "node-1");
        LeaderLatchExample node2 = new LeaderLatchExample(connStr, latchPath, "node-2");
        node1.start();
        node2.start();

        for (int i = 0; i < 3; i++) {
            System.out.println("node-1 think the leader is " + node1.currentLeader());
            System.out.println("node-2 think the leader is " + node2.currentLeader());
            Thread.sleep(10000);
        }

        node1.close();

        System.out.println("now node-2 think the leader is " + node2.currentLeader());

        node2.close();

    }

}