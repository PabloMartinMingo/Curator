package es.upm.dit.lab.ctr;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ProcessBarrier  extends Thread {

	private List<String> listBarriersP = null;
	private int npBarriers = 0;
	private String rootBarrier = "/b1";
	private CuratorFramework client;
	private Watcher barrierWatcherP;
	private Integer mutex;

	public ProcessBarrier(CuratorFramework client, Watcher barrierWatcherP, Integer mutex) {
		this.client = client;
		this.barrierWatcherP = barrierWatcherP;
		this.mutex = mutex;			
	}

	@Override
	public void run() {
		Stat s = null;
		while (true) {
			try {
				synchronized (mutex) {
					mutex.wait();
				}
				GetChildrenBuilder children = client.getChildren();
				listBarriersP = children.forPath(rootBarrier);
				npBarriers ++;
				//System.out.println("Process Barrier. NBarriers: " + npBarriers);
				System.out.println("Current # of processes waiting: " + listBarriersP.size());
			} catch (Exception e) {
				System.out.println("Unexpected Exception process barrier");
				break;
			}	
		}
	}
}