package es.upm.dit.lab.ctr;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;

import com.google.common.collect.Lists;


public class CuratorLeaderElection{
    private static final int CLIENT_QTY=10;
    private static final String PATH="/members";
public static void main(String[] args){
	List<CuratorFramework> clients=Lists.newArrayList();
	List<LeaderLatch> members=Lists.newArrayList();
    try {
        //server=new TestingServer();
        //10 LeaderLatch are created, one of them will be elected as the leader after startup 
        for(int i=0;i<CLIENT_QTY;i++){
            CuratorFramework client=CuratorFrameworkFactory.newClient("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",new ExponentialBackoffRetry(1000,3));
            clients.add(client);
            LeaderLatch leader = new LeaderLatch(client,PATH, "member-000000000"+i);
            members.add(leader);
            client.start();
            leader.start();
        }
        System.out.println("Choosing the leader from group: " + PATH + ". Wait please.");
        //Because the election will take some time, the leader cannot be obtained immediately after start. 
        Thread.sleep(20000);
        LeaderLatch currentLeader = null;
        for(int i=0;i<CLIENT_QTY;i++){
            LeaderLatch leader=members.get(i);
            //Check whether you are the leader through hasLeadership 
            if(leader.hasLeadership()){
                currentLeader=leader;
                break;
            }
        }

        System.out.println("Current leader is "+currentLeader.getId());
        Thread.sleep(5000);
        System.out.println("Timeout, releasing the current leader...");
        System.out.println("Released the leader "+currentLeader.getId());
        //The current leadership can only be released through close.
        currentLeader.close();

        //await is a blocking method, trying to get the leader status, but it may not be able to take the position. 
        members.get(0).await(2,TimeUnit.SECONDS);
        System.out.println("Trying to get the new leader");
        //You can get the ID of the current leader through .getLeader().getId()
        System.out.println("The new leader is "+members.get(0).getLeader().getId());

        System.out.println("Press enter/return to quit");
        new BufferedReader(new InputStreamReader(System.in)).readLine();
    } catch (Exception e) {
        e.printStackTrace();
    }finally{
        System.out.println("Shutting down....");
        for(LeaderLatch exampleClient: members){
            CloseableUtils.closeQuietly(exampleClient);
        }
        for(CuratorFramework client:clients){
            CloseableUtils.closeQuietly(client);
        }//CloseableUtils.closeQuietly(server);
    }

}
}

