package edu.indiana.cs.b649.wc.serial;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.regex.Pattern;

public class SerialWordCount {
    public static void main(String[] args) {
        if (args.length != 1){
            System.out.println("Please provide a text file containing the words");
            System.exit(-1);
        }

        String wordFile = args[0];
        try{
            BufferedReader br = new BufferedReader(new FileReader(wordFile));
            Hashtable<String, Integer> wordToCountTable = new Hashtable
                <String, Integer>();
            Pattern pat = Pattern.compile(" ");
            String line;
            String [] splits;
            while ((line = br.readLine()) != null){
                splits = pat.split(line);
                for (String s:splits){
                    if (wordToCountTable.containsKey(s)) {
                        wordToCountTable.put(s, wordToCountTable.get(s)+1);
                        continue;
                    }
                    wordToCountTable.put(s, 1);
                }
            }

            Enumeration<String> words = wordToCountTable.keys();
            String key;
            while(words.hasMoreElements()){
                key = words.nextElement();
                System.out.println(key + " " + wordToCountTable.get(key));
            }
        }
        catch (FileNotFoundException e) {
            System.out.println("Error! file does not exist - " + wordFile);
        }
        catch (IOException e) {
            System.out.println("Error! IO exception occurred while reading file - " + wordFile);
        }
    }
}
