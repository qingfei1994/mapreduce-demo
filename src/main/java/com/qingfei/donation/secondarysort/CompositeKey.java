package com.qingfei.donation.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ASUS on 3/14/2018.
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    public String city;
    public String state;
    public float total;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(state);
        dataOutput.writeUTF(city);
        dataOutput.writeFloat(total);
    }

    public void readFields(DataInput dataInput) throws IOException {
        state = dataInput.readUTF();
        city = dataInput.readUTF();

        total = dataInput.readFloat();
    }

    public int compareTo(CompositeKey o) {
        int compare = state.toLowerCase().compareTo(o.state.toLowerCase());
        if (compare == 0) {
            compare = city.toLowerCase().compareTo(o.city.toLowerCase());
        }
        if (compare == 0 ){
            Float.compare(total,o.total);
        }
        return compare;
    }

    @Override
    public String toString() {
        return "[state:" + state +", city:" + city+", total:"+total+"]";
    }
}
