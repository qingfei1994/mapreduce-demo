package com.qingfei.donation.filter;

import com.qingfei.donation.DonationWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

/**
 * Created by ASUS on 4/8/2018.
 */
public class SimpleRandomSamplingApp {
    public static void main(String[] args) {

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
