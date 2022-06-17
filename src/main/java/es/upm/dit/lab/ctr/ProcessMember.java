package es.upm.dit.lab.ctr;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.recipes.watch.PersistentWatcher;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.List;


public class ProcessMember extends Thread{

	private List<String> listMembersP = null;
	private int npMembers = 0;
	private String rootMembers = "/members";
	private CuratorFramework client;
	private PersistentWatcher memberWatcherP;
	private Integer mutex;

	public ProcessMember(CuratorFramework client, PersistentWatcher memberWatcherP, Integer mutex) {
		this.client = client;
		this.memberWatcherP = memberWatcherP;
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
				GetChildrenBuilder children =  (GetChildrenBuilder) client.getChildren().usingWatcher((Watcher) memberWatcherP);
				listMembersP = children.forPath(rootMembers);
				npMembers ++;
				System.out.println("Current # of members: " + listMembersP.size());
			} catch (Exception e) {
				System.out.println("Unexpected Exception process member");
				break;
			}
		}		
	}
}