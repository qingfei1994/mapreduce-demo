package com.qingfei.donation.median;

import com.qingfei.donation.DonationWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 计算每个州的捐款的中位数和标准差
 * Created by ASUS on 3/31/2018.
 * hadoop jar donor.jar com.qingfei.donation.median.MedianStdDevApp /test/donation.seqfile /test/median_std_dev
 */
public class MedianStdDevApp {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration(),"Median&Standard Deviation");
        job.setJarByClass(MedianStdDevApp.class);
        job.setMapperClass(MedianStdDevMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setReducerClass(MedianStdDevReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MedianStdDevTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class MedianStdDevMapper extends Mapper<Text,DonationWritable,Text,FloatWritable> {
        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            Text state =new Text();
            FloatWritable total = new FloatWritable();
            if (value.donorState==null||value.donorState.isEmpty()) {
                return;
            }
            state.set(value.donorState);
            total.set(value.total);
            context.write(state,total);
        }
    }

    public static class MedianStdDevReducer extends Reducer<Text,FloatWritable,Text,MedianStdDevTuple> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum=0;
            float count=0;
            MedianStdDevTuple result = new MedianStdDevTuple();
            //用一个临时的集合来存储，用于排序。
            List<Float> totals = new ArrayList<Float>();
            for (FloatWritable value:values) {
                sum+=value.get();
                count+=1;
                totals.add(value.get());

            }
            Collections.sort(totals);
            //如果有偶数个值，中位数为中间两个数的平均数
            if (count%2==0) {
                result.setMedian((totals.get((int)count/2)+totals.get((int)count/2-1))/2.0f);

            } else {
                //如果有奇数个值，中位数为中间值
                result.setMedian(totals.get((int)count/2));
            }

            //下面计算标准差
            float mean =sum/(float)count;
            float sumOfSquare = 0.0f;
            for (Float f:totals) {
                sumOfSquare += (f-mean)*(f-mean);
            }
            result.setStdDev((float)Math.sqrt(sumOfSquare/count));
            context.write(key,result);


        }
    }

}
