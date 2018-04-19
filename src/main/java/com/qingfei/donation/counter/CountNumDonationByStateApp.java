package com.qingfei.donation.counter;

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
import java.util.Arrays;
import java.util.HashSet;

/**计算每个state的捐献数量
 *
 * Created by ASUS on 4/8/2018.
 */
public class CountNumDonationByStateApp {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration(),"Count num of donation by state");
        job.setJarByClass(CountNumDonationByStateApp.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(CountNumDonationsByStateMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int code = job.waitForCompletion(true)?0:1;
        if (code ==0) {
            for (Counter counter:job.getCounters().getGroup(CountNumDonationsByStateMapper.STATE_COUNTER_GROUP)) {
                System.out.println(counter.getDisplayName()+"\t"+counter.getValue());
            }
        }
        System.exit(code);
    }

    public static class CountNumDonationsByStateMapper extends Mapper<Text,DonationWritable,NullWritable,NullWritable> {
        public static final String STATE_COUNTER_GROUP="State";
        public static final String UNKNOWN_COUNTER="unknown";
        public static final String NULL_OR_EMPTY_COUNTER="Null or Empty";

        private String[] statesArray = new String[] {"AL","AK","AZ","AR","CA",
                "CO","CT","DE","FL","GA","HI","ID","IL","IN",
                "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS",
                "MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
                "OH","OK","OR","PA","RI","SC","SF","TN","TX","UT",
                "VT","VA","WA","WV","WI","WY"
        };
        private HashSet<String> states = new HashSet<String>(Arrays.asList(statesArray));
        @Override
        protected void map(Text key, DonationWritable value, Context context) throws IOException, InterruptedException {
            if (states.contains(value.donorState)) {
                //getCounter接收两个参数，一个是group name，一个是counter name
                context.getCounter(STATE_COUNTER_GROUP,value.donorState).increment(1);
            } else if (value.donorState!=null&&!value.donorState.isEmpty()){
                context.getCounter(STATE_COUNTER_GROUP, UNKNOWN_COUNTER).increment(1);
            } else {
                context.getCounter(STATE_COUNTER_GROUP,NULL_OR_EMPTY_COUNTER).increment(1);
            }
        }
    }
}
