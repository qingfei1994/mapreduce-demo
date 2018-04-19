package com.qingfei.donation.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ASUS on 3/18/2018.
 */
public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
       CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey)b;
        return key1.state.compareTo(key2.state);
    }
}
