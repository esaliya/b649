package edu.indiana.cs.b649.wc.threaded;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.regex.Pattern;

public class WordCountWorker implements Runnable {
    private final Hashtable partialCount;
    private final ArrayList<String> lines;
    private final int offset;
    private final int count;
    private final Pattern pat = Pattern.compile(" ");

    public WordCountWorker(Hashtable partialCount, ArrayList<String> lines, int offset, int count) {
        this.partialCount = partialCount;
        this.lines = lines;
        this.offset = offset;
        this.count = count;
    }

    public void run() {
        String line;
        String[] splits;
        for (int i = offset; i < offset+count; ++i){
            line = lines.get(i);
            splits = pat.split(line);
            for (String s:splits){
                if (partialCount.containsKey(s)) {
                    partialCount.put(s, ((Integer)partialCount.get(s))+1);
                    continue;
                }
                partialCount.put(s, 1);
            }
        }
    }
}
