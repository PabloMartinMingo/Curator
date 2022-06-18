//DUDAS: 
package es.upm.dit.lab.ctr;

import java.util.Iterator;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.RetryForever;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;


public class CuratorMember implements Watcher{
	private static String rootMembers = "/members";
	private static String aMember = "/member-"; 
	//private static String groupMemberId = "1234";
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 2000;
	
	//NO FUNCIONAN LOS LOGGERS
//	String userDirectory = System.getProperty("user.dir");
//	static Logger logger = Logger.getLogger(CuratorMember.class);
	// IF ERROR, SET THE CORRECT PATH OF A VALID log4j.properties 
	//String log4jConfPath = userDirectory + "/src/main/java/es/upm/dit/lab/ctr/log4j.properties";
	//boolean config_log4j = false;
	
	final static CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
	PersistentWatcher persistentWatcher = new PersistentWatcher(client, rootMembers, true);
	
	
	public CuratorMember(){
		/*if (config_log4j) {
			try {
				System.out.println(log4jConfPath);
				// Configure Logger
				//BasicConfigurator.configure();
				PropertyConfigurator.configure(log4jConfPath);
				logger.setLevel(Level.TRACE);//(Level.INFO);			
			} catch (Exception E){
				System.out.println("The path of log4j.properties is not correct");
			}
		}*/
		//logger.error("Hola que tal");
		
		System.out.println("Starting Curator..."); 
		client.start();
		System.out.println("Session Connecting...");
		client.watchers();
		try {
			client.blockUntilConnected();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		
        
        
		if (client != null) {
			//Comprobar si existe el nodo padre: members
			Stat s;
			try {
				s = client.checkExists().forPath(rootMembers);
				if (s == null) {
					
					  PersistentNode parentNode = new PersistentNode(client, CreateMode.PERSISTENT, false, rootMembers, new byte[0]);
					  parentNode.start(); 
					 //String parentNode = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(rootMembers);
					 System.out.println("Parent node created:" + rootMembers);
					 parentNode.close();
				}else {
					System.out.println("Parent node " + rootMembers + "already exists");
				}
				
				//Creamos los nodos hijos
				client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootMembers + aMember);
				
				GetChildrenBuilder children = client.getChildren();
	            for (String a : children.forPath(rootMembers)) {
	            	System.out.println("Member created: " + a);
	            }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//Si no existe, lo creamos, y si existe lo decimos
			
                
		}
		
		persistentWatcher.start();
        Listenable<Watcher> listenable = persistentWatcher.getListenable();
        listenable.addListener(new Watcher() {
            public void process(WatchedEvent event) {
            	System.out.println("------------------Watcher Member------------------\n");		
                GetChildrenBuilder children = client.getChildren();
                try {
                	System.out.println("        Update!!");
                    List<String> childrenList = children.forPath(rootMembers);
                    printListMembers(childrenList);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
		
		
	}
	
	public void process(WatchedEvent event) {
		try {
			 GetChildrenBuilder children = client.getChildren();
             List<String> childrenList = children.forPath(rootMembers);
             System.out.println("List of members alive:" + childrenList);
             //printListMembers(childrenList);
              
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	private static void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			//System.out.println("List of members alive:");	
			System.out.print(string + ", ");				
		}
		System.out.println();

	}
	
	
	public static void main(String[] args) throws Exception {
		CuratorMember curator = new CuratorMember();
		System.out.println("Before Sleep");
		try {
			Thread.sleep(300000); 			
		} catch (Exception e) {
			System.out.println("Exception in the sleep in main");
		}
		
		 System.out.println("Closing..."); 
		 client.close();
		 
		
	}
}

