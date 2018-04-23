package com.qingfei.donation.filter;

import com.qingfei.donation.DonationWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by ASUS on 4/23/2018.
 */
public class TopFiveDollarApp {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(),"Top 5 Dollar Amount in Donation");
        job.setJarByClass(TopFiveDollarApp.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(TopFiveMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DonationWritable.class);
        job.setReducerClass(TopFiveReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DonationWritable.class);
        if (args==null||args.length!=2) {
            System.out.println("please enter first param as input path, second param as output dir.");
            System.exit(1);
        }
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class TopFiveMapper extends Mapper<Text,DonationWritable,NullWritable,DonationWritable> {
        //TreeMap是根据key排序的，用于存储top 5的数据
        private TreeMap<Float,DonationWritable> topFiveDollar = new TreeMap<Float, DonationWritable>();
        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            topFiveDollar.put(value.dollarAmount,value);
            //如果集合中包含5个以上的元素，就把集合中的最小值删除
            if (topFiveDollar.size()>5) {
                topFiveDollar.remove(topFiveDollar.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (DonationWritable donation: topFiveDollar.values()) {
                context.write(NullWritable.get(), donation);
            }
        }
    }

    /*
        Reduce做的事情和Map阶段基本一样
     */
    public static class TopFiveReducer extends Reducer<NullWritable,DonationWritable,NullWritable,DonationWritable> {
        private TreeMap<Float,DonationWritable> top5Dollar = new TreeMap<Float, DonationWritable>();
        @Override
        protected void reduce(NullWritable key, Iterable<DonationWritable> values, Context context) throws IOException, InterruptedException {
            for (DonationWritable donation:values) {
                top5Dollar.put(donation.dollarAmount,donation);
                //如果集合包含5个以上的元素，就把集合中的最小值删除
                if (top5Dollar.size()>5) {
                    top5Dollar.remove(top5Dollar.firstKey());
                }
            }
            //最后输出top 5的记录
            for (DonationWritable donation:top5Dollar.values()) {
                context.write(NullWritable.get(),donation);
            }
        }
    }
}
