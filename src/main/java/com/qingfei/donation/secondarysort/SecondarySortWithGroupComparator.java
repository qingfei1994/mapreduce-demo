package com.qingfei.donation.secondarysort;

import com.qingfei.donation.DonationWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/** With 3 reducers using customized partitioner and GroupComparator
 *
 * Created by ASUS on 3/14/2018.
 */
public class SecondarySortWithGroupComparator {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration(),"secondary sort");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setJarByClass(SecondarySortWithGroupComparator.class);
        job.setMapperClass(CompositeKeyCreationMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(DonationWritable.class);
        job.setPartitionerClass(CompostieKeyPartitioner.class);
        job.setNumReduceTasks(3);
        job.setSortComparatorClass(SecordarySortComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class CompositeKeyCreationMapper extends Mapper<Text,DonationWritable,CompositeKey,DonationWritable> {
        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            CompositeKey compositeKey = new CompositeKey();
            //因为数据量有点小所以把donorcity为空的算进来了
            if ("".equals(value.donorState)) {
                return;
            }
            compositeKey.city = value.donorCity;
            compositeKey.state = value.donorState;
            compositeKey.total = value.total;
            context.write(compositeKey,value);
        }
    }
    public static class SecordarySortComparator extends WritableComparator{
        protected SecordarySortComparator() {
            super(CompositeKey.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            CompositeKey key1 = (CompositeKey)a;
            CompositeKey key2 = (CompositeKey)b;
            int compare = key1.state.toLowerCase().compareTo(key2.state.toLowerCase());
            if (compare == 0) {
                compare = key1.city.toLowerCase().compareTo(key2.city.toLowerCase());
            }
            if (compare == 0 ){
                compare = -1 * Float.compare(key1.total,key2.total);
            }
            return compare;
        }
    }
    //保证state相同的记录由同一个reducer来处理
    public static class CompostieKeyPartitioner extends Partitioner<CompositeKey,DonationWritable> {
        @Override
        public int getPartition(CompositeKey compositeKey, DonationWritable donationWritable, int numPartitions) {
             //为什么这里要用与运算?
            return Math.abs(compositeKey.state.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


}
