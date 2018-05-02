package com.qingfei.donation.filter;

import com.qingfei.donation.DonationWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * Created by ASUS on 4/8/2018.
 */
public class SimpleRandomSamplingApp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("filter_percentage",args[0]);
        Job job = Job.getInstance(conf,"Random Sample App");
        job.setJarByClass(SimpleRandomSamplingApp.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(SRSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DonationWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        int code = job.waitForCompletion(true)?0:1;
        System.exit(code);
    }

    public static class SRSMapper extends Mapper<Text,DonationWritable,NullWritable,DonationWritable> {
        private Random rands = new Random();
        private Double percentage;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //Retrieve the percentage that is passed in via the configuration
            // conf.set("filter_percentage",0.5);
            String strPercent = context.getConfiguration().get("filter_percentage");
            percentage = Double.parseDouble(strPercent)/100.0;
        }

        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            if (rands.nextDouble()<percentage) {
                context.write(NullWritable.get(),value);
            }
        }
    }
}
