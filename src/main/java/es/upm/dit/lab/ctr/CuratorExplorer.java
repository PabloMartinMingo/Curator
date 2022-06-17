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


public class CuratorExplorer implements Watcher {
    private static String rootMembers = "/members";
    private static String rootBarrier = "/b1";
    //private static String groupMemberId = "1234";
    private static final int SESSION_TIMEOUT = 5000;
    private static final int CONNECTION_TIMEOUT = 2000;

    private static int nMembers  = 0;
    private static int nBarriers = 0;

    private static Integer mutex        = -1;
    private static Integer mutexBarrier = -2;
    private static Integer mutexMember  = -3;

    //NO FUNCIONAN LOS LOGGERS
    String userDirectory = System.getProperty("user.dir");
    static Logger logger = Logger.getLogger(CuratorExplorer.class);
    // IF ERROR, SET THE CORRECT PATH OF A VALID log4j.properties
    //String log4jConfPath = userDirectory + "/src/main/java/es/upm/dit/lab/ctr/log4j.properties";
    //boolean config_log4j = false;

    public CuratorExplorer() {
    }

    public void configure(){
    	System.out.println("Hola");
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
        try {
        	client.start();
        	System.out.println("Hola2");
            client.blockUntilConnected();
            System.out.println("Hola3");
            PersistentWatcher persistentMemberWatcher = new PersistentWatcher(client, rootMembers, true);
            PersistentWatcher persistentBarrierWatcher = new PersistentWatcher(client, rootBarrier, true);
            //CAMBIAR PARA PONER UN WATCHER QUE NO ESTÉ ASIGNADO A NINGÚN PATH!!!!!!!!
            PersistentWatcher persistentExplorerWatcher = new PersistentWatcher(client, rootMembers , true);
            Listenable<Watcher> listenable0 = persistentMemberWatcher.getListenable();
            listenable0.addListener(new Watcher() {
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
            });
            Listenable<Watcher> listenable = persistentMemberWatcher.getListenable();
            listenable.addListener(new Watcher() {
                public void process(WatchedEvent event) {
                	
                    System.out.println("------------------Watcher MEMBER ------------------");
                    System.out.println("Member: " + event.getType() + ", " + event.getPath());
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
            persistentExplorerWatcher.start();
            persistentMemberWatcher.start();
            System.out.println("Hola4");
            persistentBarrierWatcher.start();
            System.out.println("Hola5");
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        try {
            synchronized(mutex) {
            	System.out.println("Hola6");
                mutex.wait();
                System.out.println("Hola7");
            }
        } catch (Exception e) {
            System.err.println("Error when stopping mutex");
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
                    rootMemberNode.close();
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

                    PersistentNode barrierNode = new PersistentNode(client, CreateMode.EPHEMERAL_SEQUENTIAL, false, rootBarrier, new byte[0]);
                    barrierNode.start();
                    System.out.println("Parent node created:" + rootMembers);
                    barrierNode.close();
                }else {
                    System.out.println("Parent node " + rootMembers + "already exists");
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        ProcessMember pm = new ProcessMember(client, memberWatcher, mutexMember);
        pm.start();
        ProcessBarrier bm = new ProcessBarrier(client, barrierWatcher, mutexBarrier);
        bm.start();
        //client.watchers();
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

//    Watcher memberWatcher = new Watcher() {
//        public void process(WatchedEvent event) {
//
//            //Stat s = null;
//
//            System.out.println("------------------Watcher MEMBER ------------------");
//            System.out.println("Member: " + event.getType() + ", " + event.getPath());
//            try {
//                if (event.getPath() == null) {
//                    //if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
//                    System.out.println("SyncConnected");
//                    synchronized (mutex) {
//                        mutex.notify();
//                    }
//                }
//                else if (event.getPath().equals(rootMembers)) {
//                    //listMembers = zk.getChildren(rootMembers, this, s);
//                    synchronized (mutexMember) {
//                        nMembers ++;
//                        System.out.println("# of Members watchers: " + nMembers);
//                        mutexMember.notify();
//                    }
//                }
//                else if (event.getPath().equals(rootBarrier)) {
//                    //listBarriers = zk.getChildren(rootBarrier, this.barrierWatcher, s);
//                    synchronized (mutexBarrier) {
//                        nBarriers ++;
//                        System.out.println("Unexpeted to handle this watcher. MW NBarriers: " + nBarriers);
//                        mutexBarrier.notify();
//                    }
//                } else {
//                    System.out.println("Member: Received a watcher with a path not expected");
//                }
//
//                //System.out.println("-----------------------------------------------");
//            } catch (Exception e) {
//                System.out.println("Unexpected Exception process");
//            }
//        }
//    };

//    Watcher barrierWatcher = new Watcher() {
//        public void process(WatchedEvent event) {
//            //Stat s = null;
//
//            System.out.println("------------------Watcher BARRIER ------------------");
//            System.out.println("Barrier: " + event.getType() + ", " + event.getPath());
//            try {
//                if (event.getPath() == null) {
//                    //if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
//                    System.out.println("SyncConnected");
//                    synchronized (mutex) {
//                        mutex.notify();
//                    }
//                }
//                else if (event.getPath().equals(rootMembers)) {
//                    //listMembers = zk.getChildren(rootMembers, process, s);
//                    //&synchronized (mutexMember) {
//                    synchronized (mutexMember) {
//                        nMembers ++;
//                        System.out.println("Unexpeted to handle this watcher. BW Members: " + nMembers);
//                        mutexMember.notify();
//                    }
//                }
//                else if (event.getPath().equals(rootBarrier)) {
//                    //listBarriers = zk.getChildren(rootBarrier, this.barrierWatcher, s);
//                    synchronized (mutexBarrier) {
//                        nBarriers ++;
//                        System.out.println("# of Barriers watchers: " + nBarriers);
//                        mutexBarrier.notify();
//                    }
//                } else {
//                    System.out.println("Barrier: Received a watcher with a path not expected");
//                }
//                //System.out.println("-----------------------------------------------");
//            } catch (Exception e) {
//                System.out.println("Unexpected Exception process");
//            }
//        }
//    };


    public static void main(String[] args) throws Exception {
        CuratorExplorer curator = new CuratorExplorer();
        curator.configure();
    }
}
