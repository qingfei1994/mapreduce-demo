package com.qingfei.donation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Created by ASUS on 2/22/2018.
 */
public class DonationReader {
    public static void main(String[] args) {
        Path filePath = new Path(args[0]);
        System.out.println(filePath.getName());
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(),SequenceFile.Reader.file(filePath));
            System.out.println("Compressed?"+reader.isBlockCompressed());
            Text key = new Text();
            DonationWritable value = new DonationWritable();
            System.out.println(reader.getKeyClassName());
            System.out.println(reader.getValueClassName());
            while(reader.next(key,value)) {
                System.out.println(value);

            }
        } catch (Exception e) {

        }
    }
}
