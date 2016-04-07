package edu.indiana.cs.b649.pagerank.parallel;

public class ParallelPageRank {
    public static void main(String[] args) {
        String inputDirectory = args[0];
        int numUrls = Integer.parseInt(args[1]);
        int numTasks = Integer.parseInt(args[2]);
        int numIterations = Integer.parseInt(args[3]);

        Communicator comm = new Communicator();
        PageRankWorker[] workers = new PageRankWorker[numTasks];
        for (int i = 0; i < numTasks; ++i){
            workers[i] = new PageRankWorker(numUrls, i, inputDirectory, numIterations, comm);
            new Thread(workers[i]).start();
        }


    }
}
