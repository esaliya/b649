package edu.indiana.cs.b649.pagerank.parallel;

import com.google.common.base.Strings;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class PageRankWorker implements Runnable {
    int numUrls;
    int taskId;
    String inputDirectory;
    int numIterations;
    Communicator comm;

    Map<Integer, ArrayList<Integer>>
        partialGraph = new HashMap<>();

    Map<Integer, Double> currentPRTable = new TreeMap<>();
    Map<Integer, Double> finalPRTable = new TreeMap<>();

    public PageRankWorker(int numUrls, int taskId, String inputDirectory, int numIterations, Communicator comm) {
        this.numUrls = numUrls;
        this.taskId = taskId;
        this.inputDirectory = inputDirectory;
        this.numIterations = numIterations;
        this.comm = comm;
    }

    public void computePageRank(){
        for (Map.Entry<Integer, ArrayList<Integer>> entry : partialGraph.entrySet()) {
            Integer sourceUrl = entry.getKey();
            ArrayList<Integer> targetUrls = entry.getValue();
            if (targetUrls == null) {
                // simply assume that the IDs of pages are: 0,1,2,...,(numUrls-1)
                double pr = currentPRTable.get(sourceUrl) * 1.0 / numUrls;
                for (int i = 0; i < numUrls; i++) {
                    if (finalPRTable.containsKey(i)){
                        finalPRTable.put(i, pr+(finalPRTable.get(i)));
                    } else {
                        finalPRTable.put(i, pr);
                    }
                }

            }
            else {
                int numOfOutLinks = targetUrls.size();
                double pr = currentPRTable.get(sourceUrl) * 1.0 / numOfOutLinks;
                for (Integer target : targetUrls) {
                    if (finalPRTable.containsKey(target)){
                        finalPRTable.put(target, pr+(finalPRTable.get(target)));
                    } else {
                        finalPRTable.put(target, pr);
                    }
                }
            }
        }
    }

    public void computeInitialPageRanks(){
        for (Map.Entry<Integer, ArrayList<Integer>> entry : partialGraph.entrySet()) {
            int sourceUrl = entry.getKey();
            currentPRTable.put(sourceUrl, 1.0 / numUrls);
        }
    }

    public void loadPartialGraph(){
        Pattern pat = Pattern.compile("[ \t]");
        try(BufferedReader reader = Files.newBufferedReader(Paths.get(inputDirectory, "pr_" + taskId))){
            String line;
            String[] splits;
            while ((line = reader.readLine()) != null){
                if (Strings.isNullOrEmpty(line)) continue;
                splits = pat.split(line);

                if(splits.length == 1){
                    partialGraph.put(Integer.parseInt(splits[0]), null);
                }else{
                    ArrayList<Integer> targetUrls = new ArrayList<>();
                    for(int i=1; i<splits.length; i++){
                        targetUrls.add(Integer.parseInt(splits[i]));
                    }
                    partialGraph.put(Integer.parseInt(splits[0]), targetUrls);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        loadPartialGraph();
        computeInitialPageRanks();
        currentPRTable = comm.allGather(currentPRTable);
        for (int i = 0; i < numIterations; ++i) {
            computePageRank();
            finalPRTable = comm.allReduce(finalPRTable);
            scaleByDampingFactor();
            currentPRTable = finalPRTable;
            finalPRTable = new TreeMap<>();
        }

        if (taskId == 0){
            printFinalPageRanks();
        }

    }

    private void scaleByDampingFactor() {
        for (Map.Entry<Integer, Double> entry : finalPRTable.entrySet()) {
            int sourceUrl = entry.getKey();
            double pr = entry.getValue();

            pr = 0.85*pr+0.15*(1.0)/(double)numUrls;
            finalPRTable.put(sourceUrl, pr);
        }
    }

    private void printFinalPageRanks() {
        for (Map.Entry<Integer, Double> e: finalPRTable.entrySet()){
            System.out.println(e.getKey() + " " + e.getValue());
        }
    }
}
