import org.apache.zookeeper.WatchedEvent;

import java.io.File;
import java.io.IOException;

public class ClusterHealer {

    // Path to the worker jar
    private final String pathToProgram;
    // The number of worker instances we need to maintain at all times
    private final int numberOfWorkers;

    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    public void initialiseCluster() {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Instantiates a Zookeeper client, creating a connection to the Zookeeper server.
     */
    public void connectToZookeeper() {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Keeps the application running waiting for Zookeeper events.
     */
    public void run() {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Closes the Zookeeper client connection.
     */
    public void close() {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Handles Zookeeper events related to: - Connecting and disconnecting from the Zookeeper server. - Changes in the
     * number of workers currently running.
     *
     * @param event A Zookeeper event
     */
    public void process(WatchedEvent event) {
        // **************
        // YOUR CODE HERE
        // **************
    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() {
        // **************
        // YOUR CODE HERE
        // **************
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
