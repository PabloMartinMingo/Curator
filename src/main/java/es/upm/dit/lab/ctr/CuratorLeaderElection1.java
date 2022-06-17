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

import com.google.common.collect.Lists;


public class CuratorLeaderElection1{
    private static final int CLIENT_QTY=10;
    private static final String PATH="/examples/leader";
public static void main(String[] args){
    List<CuratorFramework> clients=Lists.newArrayList();
    List<LeaderLatch> examples=Lists.newArrayList();
    //TestingServer server = null;
    try {
        //server=new TestingServer();
        //10 LeaderLatch are created, one of them will be elected as the leader after startup 
        for(int i=0;i<CLIENT_QTY;i++){
            CuratorFramework client=CuratorFrameworkFactory.newClient("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183",new ExponentialBackoffRetry(1000,3));
            clients.add(client);
            LeaderLatch example=new LeaderLatch(client,PATH,"client #"+i);
            examples.add(example);
            client.start();
            example.start();
        }

        //Because the election will take some time, the leader cannot be obtained immediately after start. 
        Thread.sleep(20000);
        LeaderLatch currentLeader=null;
        for(int i=0;i<CLIENT_QTY;i++){
            LeaderLatch example=examples.get(i);
            //Check whether you are the leader through hasLeadership 
            if(example.hasLeadership()){
                currentLeader=example;
                break;
            }
        }

        System.out.println("current leader is "+currentLeader.getId());
        System.out.println("release the leader "+currentLeader.getId());
        //The current leadership can only be released through close.
        currentLeader.close();

        //await is a blocking method, trying to get the leader status, but it may not be able to take the position. 
        examples.get(0).await(2,TimeUnit.SECONDS);
        System.out.println("client #0 maybe is elected as the leader or not although it want to be");
        //You can get the ID of the current leader through .getLeader().getId()
        System.out.println("the new leader is "+examples.get(0).getLeader().getId());

        System.out.println("Press enter/return to quit/n");
        new BufferedReader(new InputStreamReader(System.in)).readLine();
    } catch (Exception e) {
        e.printStackTrace();
    }finally{
        System.out.println("Shutting down....");
        for(LeaderLatch exampleClient: examples){
            CloseableUtils.closeQuietly(exampleClient);
        }
        for(CuratorFramework client:clients){
            CloseableUtils.closeQuietly(client);
        }
        //CloseableUtils.closeQuietly(server);
    }

}
}