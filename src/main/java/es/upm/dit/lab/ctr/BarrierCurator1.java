//MEDIO FUNCIONA, NO ES LO MÁS BONITO DEL MUNDO PERO FUNCIONA

package es.upm.dit.lab.ctr;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;


public class BarrierCurator1 implements Watcher{
	
	//Variables
	int nWatchers;
	Integer mutexBarrier = -1;
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 2000;
	private static String rootBarrier = "/b1";
	private static int QTY = 3;
	private static String name; 
	

	public void process(WatchedEvent event) {
		nWatchers++;
		System.out.println("  Watcher event: " + event.toString() + ", " + nWatchers);
		//System.out.println("Process: " + event.getType());
		synchronized (mutexBarrier) {
			mutexBarrier.notify();
		}
	}
	
	public static void main(String args[]) {
		CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
		System.out.println("Starting Curator..."); 
		client.start();
		System.out.println("Session Connecting...");
		
		
		if (client != null) {
			//Comprobar si existe el nodo padre: products
			Stat s;
			try {
				s = client.checkExists().forPath(rootBarrier);
				//Si no existe, lo creamos, y si existe lo decimos
				if (s == null) {
					
					  PersistentNode parentNode = new PersistentNode(client, CreateMode.PERSISTENT, false, rootBarrier, new byte[0]);
					  parentNode.start(); 
					 //String parentNode = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootMembers);
					 System.out.println("Parent node created:" + rootBarrier);
					 parentNode.close();
				}else {
					System.out.println("Parent node" + " " + rootBarrier + " " + "already exists");
				}
			} catch (Exception e) {
				System.out.println("No se ha conseguido comprobar si el nodo padre existe");
			}
			
                
		}
		
		// My node name
				try {
					//hostname
					name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
				} catch (UnknownHostException e) {
					System.out.println(e.toString());
				}
			
		
		final DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, rootBarrier, QTY);
		try {
			barrier.enter();
			client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootBarrier + "/" + name);
			GetChildrenBuilder children = client.getChildren();
            for (String a : children.forPath(rootBarrier)) {
            	System.out.println("Process waiting: " + a);
            }
			System.out.println("Processes entering the barrier:" + args[1]);
		} catch (Exception e) {
			System.out.println("Error entering the barrier");
		}
		
		Random rand = new Random();
		int r = rand.nextInt(100);
		// Loop for rand iterations
		for (int i = 0; i < r; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				System.out.println("Exception in the sleep in main");
			}
		}
		
		try {
			client.delete().forPath(rootBarrier + "/" + name);
			barrier.leave();
			Thread.sleep(3000);
		} catch (Exception e) {
			System.out.println("Waiting to leave the barrier");
		}
		System.out.println("Left the barrier");
//		try {
//			client.delete().forPath(rootBarrier + "/ready");
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
}