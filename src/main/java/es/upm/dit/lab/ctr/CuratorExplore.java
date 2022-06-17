package es.upm.dit.lab.ctr;

import es.upm.dit.lab.ctr.ProcessBarrier;

import es.upm.dit.lab.ctr.ProcessMember;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.curator.retry.RetryForever;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.List;


public class CuratorExplore implements Watcher {
	private static String rootMembers = "/members";
    private static String rootBarrier = "/b1";
    //private static String groupMemberId = "1234";
    private static final int SESSION_TIMEOUT = 5000;
    private static final int CONNECTION_TIMEOUT = 2000;

    private static int nMembers  = 0;
    private static int nBarriers = 0;
    private static String aMember = "/member-"; 

    private static Integer mutex        = -1;
    private static Integer mutexBarrier = -2;
    private static Integer mutexMember  = -3;
    
    private List<String> listMembersP = null;
    private List<String> listBarriersP = null;
	private int npMembers = 0;
    
    CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
    PersistentWatcher persistentMemberWatcher = new PersistentWatcher(client, rootMembers, true);
    PersistentWatcher persistentBarrierWatcher = new PersistentWatcher(client, rootBarrier, true);
    
    
    public CuratorExplore() {
    	Listenable<Watcher> listenable = persistentMemberWatcher.getListenable();
        listenable.addListener(new Watcher() {
            public void process(WatchedEvent event) {
            	
                System.out.println("------------------Watcher MEMBER ------------------");
                System.out.println("Member: " + event.getType() + ", " + event.getPath());
//                GetChildrenBuilder children =  client.getChildren();
//				try {
//					listMembersP = children.forPath(rootMembers);
//				} catch (Exception e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//				npMembers ++;
//				System.out.println("Current # of members: " + listMembersP.size());
                try {
                    if (event.getPath() == null) {
                        //if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                        System.out.println("SyncConnected");
                        synchronized (mutex) {
                            mutex.notify();
                        }
                    }
                    else if (event.getPath().equals(rootMembers)) {
                        //listMembers = zk.getChildren(rootMembers, this, s);
                        synchronized (mutexMember) {
                            nMembers ++;
                            System.out.println("# of Members watchers: " + nMembers);
                            mutexMember.notify();
                        }
                    }
                    else if (event.getPath().equals(rootBarrier)) {
                        //listBarriers = zk.getChildren(rootBarrier, this.barrierWatcher, s);
                        synchronized (mutexBarrier) {
                            nBarriers ++;
                            System.out.println("Unexpeted to handle this watcher. MW NBarriers: " + nBarriers);
                            mutexBarrier.notify();
                        }
                    } else {
                        System.out.println("Member: Received a watcher with a path not expected");
                    }

                    //System.out.println("-----------------------------------------------");
                } catch (Exception e) {
                    System.out.println("Unexpected Exception process");
                }
            }
        });

        Listenable<Watcher> listenable2 = persistentBarrierWatcher.getListenable();
        listenable2.addListener(new Watcher() {
            public void process(WatchedEvent event) {
                System.out.println("------------------Watcher BARRIER ------------------");
                System.out.println("Barrier: " + event.getType() + ", " + event.getPath());
                GetChildrenBuilder children =  client.getChildren();
				try {
					listBarriersP = children.forPath(rootBarrier);
				} catch (Exception e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				npMembers ++;
				System.out.println("Current # of barriers: " + listBarriersP.size());
                try {
                    if (event.getPath() == null) {
                        //if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                        System.out.println("SyncConnected");
                        synchronized (mutex) {
                            mutex.notify();
                        }
                    }
                    else if (event.getPath().equals(rootMembers)) {
                        //listMembers = zk.getChildren(rootMembers, process, s);
                        //&synchronized (mutexMember) {
                        synchronized (mutexMember) {
                            nMembers ++;
                            System.out.println("Unexpeted to handle this watcher. BW Members: " + nMembers);
                            mutexMember.notify();
                        }
                    }
                    else if (event.getPath().equals(rootBarrier)) {
                        //listBarriers = zk.getChildren(rootBarrier, this.barrierWatcher, s);
                        synchronized (mutexBarrier) {
                            nBarriers ++;
                            System.out.println("# of Barriers watchers: " + nBarriers);
                            mutexBarrier.notify();
                        }
                    } else {
                        System.out.println("Barrier: Received a watcher with a path not expected");
                    }
                    //System.out.println("-----------------------------------------------");
                } catch (Exception e) {
                    System.out.println("Unexpected Exception process");
                }
            }
        });
        	
        persistentMemberWatcher.start();
        persistentBarrierWatcher.start();
        
        
    	
    }
    
    public void configure(){
    	try {
    		System.out.println("Starting Curator..."); 
    		client.start();
    		System.out.println("Session Connecting...");
//    		try {
//    			client.blockUntilConnected();
//    		} catch (Exception e1) {
//    			// TODO Auto-generated catch block
//    			e1.printStackTrace();
//    		}
    		 try {
    	            synchronized(mutex) {
    	                mutex.wait();
    	                System.out.println("Hola7");
    	            }
    	        } catch (Exception e) {
    	            System.err.println("Error when stopping mutex");
    	        }
    	}catch (Exception e) {
			System.out.println("Exception in constructor");
		}
    	
		 if (client != null) {
	            //Comprobar si existe el nodo padre: members
	            Stat s;
	            try {
	                s = client.checkExists().forPath(rootMembers);
	                if (s == null) {

	                    PersistentNode rootMemberNode = new PersistentNode(client, CreateMode.PERSISTENT, false, rootMembers, new byte[0]);
	                    rootMemberNode.start();
	                    System.out.println("Parent node created:" + rootMembers);
	                    //rootMemberNode.close();
	                }else {
	                    System.out.println("Parent node " + rootMembers + "already exists");
	                }
	            } catch (Exception e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }

	            try {
	                s = client.checkExists().forPath(rootBarrier);
	                if (s == null) {
	                	//client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(rootMembers);
	                	PersistentNode barrierNode = new PersistentNode(client, CreateMode.PERSISTENT, false, rootBarrier, new byte[0]);
	                	barrierNode.start();	       
	                	//barrierNode.close();
	                }else {
	                    System.out.println("Parent node " + rootMembers + "already exists");
	                }
	            } catch (Exception e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }

	        }
		 
		 		 
		 	ProcessMember pm = new ProcessMember(client, persistentMemberWatcher, mutexMember);
	        pm.start();
//	        ProcessBarrier bm = new ProcessBarrier(client, (Watcher) persistentBarrierWatcher, mutexBarrier);
//	        bm.start();
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
   
    
    
    public static void main(String[] args) throws Exception {
    	
        CuratorExplore curator = new CuratorExplore();
        curator.configure();
    }
}
    