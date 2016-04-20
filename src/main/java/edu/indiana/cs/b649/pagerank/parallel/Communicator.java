package edu.indiana.cs.b649.pagerank.parallel;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

public class Communicator {
    Map<Integer, Double> globalPRTable = new TreeMap<>();
    boolean isDirty = false;
    public Map<Integer, Double> allGather(Map<Integer, Double> currentPRTable) {

        return globalPRTable;
    }

    public Map<Integer, Double> allReduce(Map<Integer, Double> finalPRTable){

        return finalPRTable;
    }

    private synchronized void updateGlobalPRTable(Map<Integer, Double> localPRTable){
        if (isDirty){
            globalPRTable = new TreeMap<>();
        }

        for (Map.Entry<Integer, Double> entry : localPRTable.entrySet()) {
            int sourceUrl = entry.getKey();
            double pr = entry.getValue();

            if (globalPRTable.containsKey(sourceUrl)){
                globalPRTable.put(sourceUrl, pr+(globalPRTable.get(sourceUrl)));
            } else {
                globalPRTable.put(sourceUrl, pr);
            }
        }
        isDirty =

    }

    public void barrier(){

    }
}
