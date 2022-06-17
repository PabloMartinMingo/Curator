package es.upm.dit.lab.ctr;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.RetryForever;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;




public class ProducerCurator implements Watcher{
	private static String rootProducts = "/products";
	private static String aProduct = "/product-"; 
	static List<String> listProducts = null;
	private int nProductsMax = 0;	
	private int id = 0;
	private Integer mutex        = -1;
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 2000;

		
		final CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
		PersistentWatcher persistentWatcher = new PersistentWatcher(client, rootProducts, true);
		
		public ProducerCurator(int nProductsMax, int id) throws Exception {
			
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
			
			
	        
	        
				
				//Creamos los nodos hijos
			/*
			 * for(int i = 0; i <= nProductsMax; i++) { PersistentNode childNode = new
			 * PersistentNode(client, CreateMode.PERSISTENT_SEQUENTIAL, false, rootProducts
			 * + aProduct, new byte[0]); childNode.start();
			 * 
			 * }
			 * 
			 * 
			 * GetChildrenBuilder children = client.getChildren(); for (String a :
			 * children.forPath(rootProducts)) { System.out.println("Products created: " +
			 * a); }
			 */
	                
			}
	        
	        
		}
	/*
	 * persistentWatcher.start(); Listenable<Watcher> listenable =
	 * persistentWatcher.getListenable(); listenable.addListener(new Watcher() {
	 * public void process(WatchedEvent event) {
	 * System.out.println("------------------Watcher Process------------------\n");
	 * System.out.println("---------------------------------------------------\n");
	 * 
	 * } });
	 */
		
		
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
		
		//FALTA POR HACER
		private void produce() {

			for (int i = 0; i < nProductsMax; i++) {			
				try {

					ByteBuffer b = ByteBuffer.allocate(4);
					byte[] value;
					
					// Add child with value i
					b.putInt(id * 100 + i);
					value = b.array();
					
					// Creating all products
					PersistentNode childNode = new PersistentNode(client, CreateMode.PERSISTENT_SEQUENTIAL, false, rootProducts + aProduct, value);
					childNode.start();

					// Primera forma de printear los hijos, no funciona bien
				/*
				 * GetChildrenBuilder children = client.getChildren(); for (String a :
				 * children.forPath(rootProducts)) { System.out.println("Product created: " +
				 * a); }
				 */
					// Segunda forma de printear los hijos, no funciona bien
//					List<String> children = client.getChildren().forPath(rootProducts);
//
//					for (String child : children) {
//					    System.out.println(child);
//					}
					listProducts = client.getChildren().forPath(rootProducts);
					printListMembers(listProducts);
					
					
				} catch (Exception e) {
					System.out.println("Unexpected Exception process barrier");
					break;
				}
				
				// Generate random integer
				Random rand = new Random();
				int r = rand.nextInt(50);
				// Loop for rand iterations
				for (int j = 0; j < r; j++) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {

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
		int nProducts = 5;
		Integer id;
		if (args.length == 0) {
			System.out.println("No se ha incluido el identificador. Se usa el valor 0");
			id = 0;
		} else {
			id = Integer.parseInt(args[0]);
		}
		ProducerCurator producer = new ProducerCurator(nProducts, id);
		producer.produce();		
	}
}
