package edu.indiana.cs.b649.pagerank.parallel;

import java.util.Map;

public class Communicator {

    public Map<Integer, Double> allGather(Map<Integer, Double> currentPRTable) {

        return currentPRTable;
    }

    public Map<Integer, Double> allReduce(Map<Integer, Double> finalPRTable){

        return finalPRTable;
    }

    public void barrier(){

    }
}
