package es.upm.dit.lab.ctr;

import com.google.common.collect.Lists;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.curator.test.TestingServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

public class LeaderSelectorExample
{
    private static final int        MEMBERS_QTY = 10;

    private static final String     PATH = "/members";

    public static void main(String[] args) throws Exception
    {
        // all of the useful sample code is in ExampleClient.java

        System.out.println("Create " + MEMBERS_QTY + " clients, have each negotiate for leadership and then wait a random number of seconds before letting another leader election occur.");
        System.out.println("Notice that leader election is fair: all clients will become leader and will do so the same number of times.");

        List<CuratorFramework>  clients = Lists.newArrayList();
        List<ExampleClient>     examples = Lists.newArrayList();
        String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"};
        //TestingServer           server = new TestingServer();
        
        
        try
        {
            for ( int i = 0; i < MEMBERS_QTY; ++i )
            {
                CuratorFramework    client = CuratorFrameworkFactory.newClient("localhost:2181,localhost:2182, localhost:2183", new ExponentialBackoffRetry(1000, 3));
                clients.add(client);

                ExampleClient       member = new ExampleClient(client, PATH, "Client #" + i);
                examples.add(member);

                client.start();
                member.start();
            }

            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
        finally
        {
            System.out.println("Shutting down...");

            for ( ExampleClient exampleClient : examples )
            {
                CloseableUtils.closeQuietly(exampleClient);
            }
            for ( CuratorFramework client : clients )
            {
                CloseableUtils.closeQuietly(client);
            }

            //CloseableUtils.closeQuietly(w);
        }
    }
}