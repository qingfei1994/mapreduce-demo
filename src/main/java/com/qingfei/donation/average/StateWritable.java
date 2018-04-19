package com.qingfei.donation.median;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ASUS on 3/21/2018.
 */
public class StateWritable implements WritableComparable<StateWritable> {

    public float average;
    public int count;
    public int compareTo(StateWritable o) {

            int countCmp = Integer.compare(count,o.count);
            if (countCmp == 0) {
                return Float.compare(average,o.average);
            } else {
                return countCmp;
            }

    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeFloat(average);
        dataOutput.writeInt(count);
    }

    public void readFields(DataInput dataInput) throws IOException {

        average = dataInput.readFloat();
        count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "average:"+average+",count:"+count;
    }
}
