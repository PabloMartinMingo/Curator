package es.upm.dit.lab.ctr;

import java.io.IOException;
import java.util.Scanner;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.RetryForever;

public class CuratorCounter1 {
	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 2000;
	private static String rootCounter = "/counter";
	static Scanner input = new Scanner(System.in);
	
	public static void main(String[]args){
		CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", SESSION_TIMEOUT, CONNECTION_TIMEOUT, new RetryForever(1000));
		client.start();

		SharedCount counter = new SharedCount(client, rootCounter, 0);
		try {
			counter.start();
			System.out.println("The actual value of the counter is:");
			System.out.println(counter.getCount());
			
			//counter.trySetCount(previous, newCount);
			
			System.out.print("How many times do you want to increase the value: ");
			String times = input.next();
			int i = Integer.parseInt(times);
			counter.setCount(counter.getCount() + i);
			System.out.println("Counter increased, the value of the counter is now:");
			System.out.println(counter.getCount());
		} catch (Exception e) {
			System.out.println("Error with counter");
		}
		try {
			counter.close();
		} catch (IOException e) {
			System.out.println("Error closing");
		}

		

		
	}
	

	
	
	
}