#!/bin/bash
javac ../src/main/java/edu/indiana/cs/b649/wc/serial/SerialWordCount.java
java -cp ../src/main/java edu.indiana.cs.b649.wc.serial.SerialWordCount ../src/main/resources/words.txt
