import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ClusterHealer implements Watcher {

    // Path to the worker jar
    private final String pathToProgram;
    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;

    //address of the zookeeper server with hostname and default port number
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;

    // declare zookeeper instance to access ZooKeeper
    private ZooKeeper zooKeeper;

    // znode path
    public static final String PARENT_ZNODE = "/workers"; // Assign path to znode

    // Constructor
    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    // Ephemeral - Deleted when a session ends
    // Persistent - Persist between sessions
    public void initialiseCluster() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(PARENT_ZNODE,true); // Stat checks the path of the znode

        if(stat != null) {
            System.out.println("The znode exists and the node version is " +
                    stat.getVersion());
        } else {
            System.out.println("The znode does not exists");

            //Then create parent znode here
            String znodeFullPath = zooKeeper.create(PARENT_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("Parent znode name " + znodeFullPath);
        }
    }

    /**
     * Instantiates a Zookeeper client, creating a connection to the Zookeeper server.
     */
    //method that will create the connection to zookeeper
    public void connectToZookeeper() throws IOException {
        //create a new zookeeper object and save it in the zookeeper variable
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT, (Watcher) this);
    }

    /**
     * Keeps the application running waiting for Zookeeper events.
     */
    public void run() throws InterruptedException {
        //synchronized block
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    /**
     * Closes the Zookeeper client connection.
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * Handles Zookeeper events related to: - Connecting and disconnecting from the Zookeeper server. - Changes in the
     * number of workers currently running.
     *
     * @param event A Zookeeper event
     */
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper");
                        zooKeeper.notifyAll();
                    }
                }
                break;

                //Handle Zookeeper events related to Changes in the number of workers currently running.
            case NodeCreated:
                try {
                    System.out.println("Check running workers");
                    checkRunningWorkers();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

        }
    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    //Check how many workers are currently running. If less than the required number, then start a new worker
    public void checkRunningWorkers() throws KeeperException, InterruptedException, IOException {
        List<String> workerList = zooKeeper.getChildren(PARENT_ZNODE,this);
        int workersNumber = workerList.size();

        //Check how many workers are currently running.
        //If less than the required number, then start a new worker
        while(workersNumber <= numberOfWorkers){
            System.out.println("Currently there are " + workersNumber + " workers");

            //update workersNumber
            workersNumber+=1;

            startWorker();

            // watch the cluster to check if workers die
            // replacement workers should be started when workers die.
            // the number of running workers should always be the requested number
        }

    }

    /**
     * Starts a new worker using the path provided as a command line parameter.
     *
     * @throws IOException
     */
    public void startWorker() throws IOException {
        File file = new File(pathToProgram);
        String command = "java -jar " + file.getName();
        System.out.println(String.format("Launching worker instance : %s ", command));
        Runtime.getRuntime().exec(command, null, file.getParentFile());
    }
}
