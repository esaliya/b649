#!/bin/bash
javac ../src/main/java/edu/indiana/cs/b649/wc/parallel/ParallelWordCount.java ../src/main/java/edu/indiana/cs/b649/wc/parallel/WordCountWorker.java
java -cp ../src/main/java edu.indiana.cs.b649.wc.parallel.SerialWordCount ../src/main/resources/words.txt 4
