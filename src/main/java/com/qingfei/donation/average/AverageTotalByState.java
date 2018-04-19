package com.qingfei.donation.average;

import com.qingfei.donation.DonationWritable;
import com.qingfei.donation.median.StateWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/** 根据state分组计算total的平均值
 * Created by ASUS on 3/21/2018.
 */
public class AverageTotalByState {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(),"Average Total By State");
        job.setJarByClass(AverageTotalByState.class);
        job.setMapperClass(AverageMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StateWritable.class);
        job.setCombinerClass(AverageReducer.class);
        job.setReducerClass(AverageReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StateWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);

    }

    /**
     * Mapper输出的是(state,(total,1))
     * StateWritable包含两个field,一个是平均数,另外一个是数量
     * Mapper将每条记录输出，把total当做平均数输出，数量为1
     */
    public static class AverageMapper extends Mapper<Text,DonationWritable,Text,StateWritable> {
        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            if (value.donorState==null||value.donorState.isEmpty()) {
                return;
            }
            StateWritable stateWritable = new StateWritable();
            stateWritable.average = value.total;
            stateWritable.count = 1;
            Text text = new Text();
            text.set(value.donorState);
            context.write(text,stateWritable);
        }
    }

    /**
     * Reducer/Combiner接收到的是(state,(average1,count1),(average2,count2),(average3,count3))
     * 通过average1*count1+average2*count2+average3*count3可以得到总和，
     * count1+count2+count3得到数量，
     * 两者相除就能得到平均值。
     */
    public static class AverageReducer extends Reducer<Text,StateWritable,Text,StateWritable> {
        @Override
        protected void reduce(Text key, Iterable<StateWritable> values, Context context) throws IOException, InterruptedException {
            StateWritable result = new StateWritable();
            float total =0;
            int count = 0;
            for (StateWritable value:values) {
                       total += value.average * value.count;
                        count += value.count;

            }
            result.average= total/(float)count;
            result.count = count;
            context.write(key,result);
        }
    }

}
