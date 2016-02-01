package edu.indiana.cs.b649.statistics.distributed;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class PartialData implements Writable, Serializable{
    double partialMin, partialMax, partialSum, partialSqSum;
    int partialCount;

    public PartialData(
        double partialMin, double partialMax, double partialSum,
        double partialSqSum, int partialCount) {
        this.partialMin = partialMin;
        this.partialMax = partialMax;
        this.partialSum = partialSum;
        this.partialSqSum = partialSqSum;
        this.partialCount = partialCount;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(partialMin);
        dataOutput.writeDouble(partialMax);
        dataOutput.writeDouble(partialSum);
        dataOutput.writeDouble(partialSqSum);
        dataOutput.writeInt(partialCount);
    }

    public void readFields(DataInput dataInput) throws IOException {
        partialMin = dataInput.readDouble();
        partialMax = dataInput.readDouble();
        partialSum = dataInput.readDouble();
        partialSqSum = dataInput.readDouble();
        partialCount = dataInput.readInt();
    }
}
