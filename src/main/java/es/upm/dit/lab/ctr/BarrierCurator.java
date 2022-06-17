package es.upm.dit.lab.ctr;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryForever;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


public class BarrierCurator{
	Integer mutexBarrier = -1;
	private static String name; 
	static String root="/b1";
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 2000;
	final static CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
	
	public BarrierCurator() throws Exception {
	
		
		// Create a zNode root for managing the barrier, if it does not exist
		if (client != null) {
			//Comprobar si existe el nodo padre: products
			Stat s = client.checkExists().forPath(root);
			//Si no existe, lo creamos, y si existe lo decimos
			if (s == null) {
				
				  PersistentNode parentNode = new PersistentNode(client, CreateMode.PERSISTENT, false, root, new byte[0]);
				  parentNode.start(); 
				 //String parentNode = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootMembers);
				 System.out.println("Parent node created:" + root);
				 parentNode.close();
			}else {
				System.out.println("Parent node" + " " + root + " " + "already exists");
			}
                
		}
//		// My node name
//				try {
//					//hostname
//					name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
//				} catch (UnknownHostException e) {
//					System.out.println(e.toString());
//				}
	}
	
	public static void main (String[]args) throws Exception {
		//Create a Session
		System.out.println("Starting Curator..."); 
		client.start();
		System.out.println("Session Connecting...");
		try {
			client.blockUntilConnected();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, root, 3);
		
			barrier.enter();
			System.out.println(name + "entering the barrier");
			client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(root + name);
			GetChildrenBuilder children = client.getChildren();
            for (String a : children.forPath(root)) {
            	System.out.println("Nodes in the barrier: " + a);
            }
		
		
			barrier.leave();
			System.out.println(name + "leaving the barrier");
			client.delete().forPath(root + name);
		
		
	}
}
	
	