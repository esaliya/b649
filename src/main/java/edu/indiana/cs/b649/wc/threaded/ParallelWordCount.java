package edu.indiana.cs.b649.wc.threaded;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.regex.Pattern;

public class ParallelWordCount {
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2){
            System.out.println("Please provide a text file containing the words and the number of threads to use");
            System.exit(-1);
        }

        String wordFile = args[0];
        int numThreads = Integer.parseInt(args[1]);

        ArrayList<String> lines = readLines(wordFile);
        int[] lineOffsets = new int[numThreads];
        int[] lineCounts = new int[numThreads];

        int numLines = lines.size();
        decomposeData(numThreads, numLines, lineOffsets, lineCounts);

        Hashtable[] partialCounts = new Hashtable[numThreads];
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; ++i){
            partialCounts[i] = new Hashtable();
            threads[i] = new Thread(new WordCountWorker(partialCounts[i], lines, lineOffsets[i], lineCounts[i]));
            threads[i].start();
        }

        for (int i = 0; i < numThreads; ++i){
            threads[i].join();
        }

        mergePartialCounts(partialCounts, numThreads);

        outputResults(partialCounts[0]);
    }

    private static void mergePartialCounts(Hashtable[] partialCounts, int numThreads) {
        Hashtable all = partialCounts[0];
        Hashtable ht;
        for (int i = 1; i < numThreads; ++i){
            ht = partialCounts[i];
            Enumeration<String> keys = ht.keys();
            String key;
            while(keys.hasMoreElements()){
                key = keys.nextElement();
                if (all.containsKey(key)) {
                    all.put(key, ((Integer)all.get(key))+((Integer)ht.get(key)));
                    continue;
                }
                all.put(key, ht.get(key));
            }
        }
    }

    private static void outputResults(Hashtable wordToCountTable) {
        Enumeration<String> words = wordToCountTable.keys();
        String key;
        while(words.hasMoreElements()){
            key = words.nextElement();
            System.out.println(key + " " + wordToCountTable.get(key));
        }
    }

    private static void decomposeData(
        int numThreads, int numLines, int[] lineOffsets, int[] lineCounts) {
        int p = numLines / numThreads;
        int q = numLines % numThreads;
        int offset = 0;
        for (int i = 0; i < numThreads; ++i){
            lineCounts[i] = i < q ? p+1 : p;
            lineOffsets[i] = offset;
            offset+=lineCounts[i];
        }
    }

    private static ArrayList<String> readLines(String wordFile) {
        ArrayList<String> lines = new ArrayList<String>();
        try{
            BufferedReader br = new BufferedReader(new FileReader(wordFile));
            String line;
            while ((line = br.readLine()) != null){
                lines.add(line);
            }
        }
        catch (FileNotFoundException e) {
            System.out.println("Error! file does not exist - " + wordFile);
        }
        catch (IOException e) {
            System.out.println("Error! IO exception occurred while reading file - " + wordFile);
        }
        return lines;
    }
}
