package es.upm.dit.lab.ctr;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;


public class ConsumerCurator {
	private static String rootProducts = "/products";
	private static String aProduct = "/product-"; 
	static List<String> listProducts = null;
	private int nProductsMax = 0;	
	private int nProducts = 0;
	private int nProductsWatcher = 0;
	private int id = 0;
	private Integer mutex        = -1;
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 2000;
	
	final CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
	PersistentWatcher persistentWatcher = new PersistentWatcher(client, rootProducts, true);
	
	
	public ConsumerCurator(int nProductsMax, int id) throws Exception {
		this.nProductsMax = nProductsMax;
		this.id = id;
		
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
		
		if (client != null) {
			//Comprobar si existe el nodo padre: products
			Stat s = client.checkExists().forPath(rootProducts);
			//Si no existe, lo creamos, y si existe lo decimos
			if (s == null) {
				
				  PersistentNode parentNode = new PersistentNode(client, CreateMode.PERSISTENT, false, rootProducts, new byte[0]);
				  parentNode.start(); 
				 //String parentNode = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootMembers);
				 System.out.println("Parent node created:" + rootProducts);
				 parentNode.close();
			}else {
				System.out.println("Parent node" + " " + rootProducts + " " + "already exists");
			}
                
		}

	}
	public void process(WatchedEvent event) {
		System.out.println("------------------Watcher PROCESS ------------------");
		System.out.println("Member: " + event.getType() + ", " + event.getPath());
		try {
			if (event.getPath() == null) {			
				//if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
				System.out.println("SyncConnected");
				synchronized (mutex) {
					mutex.notify();
				}
			}
			System.out.println("-----------------------------------------------");
		} catch (Exception e) {
			System.out.println("Unexpected Exception process");
		}
	}
	
	//FALTA POR HACER EL WATCHER DE LOS PRODUCTOS
	PersistentWatcher productWatcher = new PersistentWatcher(client, rootProducts, true) {
		public void process(WatchedEvent event) { 

			Stat s = null;

			System.out.println("------------------Watcher Product ------------------");
			System.out.println("Member: " + event.getType() + ", " + event.getPath());
			try {
				if (event.getPath().equals(rootProducts)) {
				listProducts = client.getChildren().forPath(rootProducts);
						nProductsWatcher ++;
						System.out.println("# of Members watchers: " + nProductsWatcher);
						printListMembers(listProducts);
						synchronized (mutex) {
							mutex.notify();
						}
				} else {
					System.out.println("Product: Received a watcher with a path not expected");
				}

			} catch (Exception e) {
				System.out.println("Unexpected Exception process");
			}
		}
	};
	
	private void consume() {
		Stat s = null;
		String path = null;
		int data = -1;
		
		while (nProducts < nProductsMax) {
			try {
				listProducts = client.getChildren().forPath(rootProducts);
			} catch (Exception e) {
				System.out.println("Unexpected Exception process barrier");
				break;
			}
			
			if (listProducts.size() > 0) {
				try {
//					System.out.println(listProducts.get(0));
					path = rootProducts+"/"+listProducts.get(0);
//					System.out.println(path);
					GetDataBuilder productData = client.getData();
					final byte[] b = client.getData().forPath(path);
					//byte[] b = client.getData(path, false, s);
					s = client.checkExists().forPath(path);
					//s = zk.exists(path, false);
					//System.out.println(s.getVersion());
					//zk.delete(path, s.getVersion());
					client.delete().forPath(path);
					
					// Generate random delay
					Random rand = new Random();
					int r = rand.nextInt(10);
					// Loop for rand iterations
					for (int j = 0; j < r; j++) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {

						}
					}
					
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    data = buffer.getInt();
                    nProducts++;
                    System.out.println("++++ Consume. Data: " + data + "; Path: " + path + "; Number of products: " + nProducts);
				} catch (Exception e) {
					// The exception due to a race while getting the list of children, get data and delete. Another
					// consumer may have deleted a child while the previous access. Then, the exception is simply
					// implies that the data has not been produced.
					System.out.println("Exception when accessing the data");
					//System.err.println(e);
					//e.printStackTrace();
					//break;
				}
			} else {
				try {
					client.getChildren().forPath(rootProducts);
					synchronized(mutex) {
						mutex.wait();
					}
				} catch (Exception e) {
					System.out.println("Unexpected Exception process barrier");
					break;
				}
			}			
		}
	}
	
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # products:" + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();

	}
	
	public static void main(String[] args) throws Exception {
		// Read 10 items
		int nProducts = 20;
		Integer id;
		if (args.length == 0) {
			id = 0;
		} else {
			id = Integer.parseInt(args[0]);
		}
		ConsumerCurator consumer = new ConsumerCurator(nProducts, id);
		consumer.consume();
	}
	
}	