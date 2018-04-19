package com.qingfei.donation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ASUS on 3/4/2018.
 */
public class DonationSumByCity {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration(),"Sum donation by city");
        job.setJarByClass(DonationSumByCity.class);
        job.setMapperClass(CityMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setReducerClass(CityReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    /**
     *  t,NewYork,1.0
     *  y,LA,3.0
     *  t,NewYork,2.0
     *  t.LA,4.0
     */
    public static class CityMapper extends Mapper<Text,DonationWritable,Text,FloatWritable> {
        private Text text = new Text();
        private FloatWritable floatWritable = new FloatWritable();
        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            // filter the records donate by teacher
            if (!"t".equals(value.donateByTeacher)&&!"".equals(value.donorCity)) {
                text.set(value.donorCity.toUpperCase());
                floatWritable.set(value.total);
                context.write(text,floatWritable);
            }
        }
    }

    /**
     * 经过Map以后，数据输出是("NEWYORK",1.0),("NEWYORK",2.0),("LA",4.0)
     */

    public static class CityReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        private FloatWritable result = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum=0;
            for (FloatWritable value:values) {
                sum+=value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
    /**
     * 经过Reduce以后，输出的是("NEWYORK",3.0),("LA",4.0)
     */

}
