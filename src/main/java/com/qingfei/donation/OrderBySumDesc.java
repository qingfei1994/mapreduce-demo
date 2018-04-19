package com.qingfei.donation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ASUS on 3/12/2018.
 */
public class OrderBySumDesc  {

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration(),"order by sum desc");
        job.setJarByClass(OrderBySumDesc.class);
        job.setMapperClass(OrderMapper.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);//使用默认的reducer,直接将key,value输出
        job.setSortComparatorClass(DescendingFloatComparator.class);//用SortComparator来实现排序
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class OrderMapper extends Mapper<Text,Text,FloatWritable,Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            FloatWritable total = new FloatWritable();
            float temp = Float.parseFloat(value.toString());
            total.set(temp);
            context.write(total,key);
        }
    }

    public static class DescendingFloatComparator extends WritableComparator {
        public DescendingFloatComparator(){
            super(FloatWritable.class,true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            FloatWritable key1 = (FloatWritable)a;
            FloatWritable key2 = (FloatWritable)b;
            return -1*key1.compareTo(key2);
        }
    }
}
