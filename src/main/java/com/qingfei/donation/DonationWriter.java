package com.qingfei.donation;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.*;

/**
 * Created by ASUS on 2/21/2018.
 */
public class DonationWriter {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        //inputFile是本地文件系统上的文件
        File inputFile = new File(args[0]);
        //outputPath是HDFS上的文件
        Path outputPath = new Path(args[1]);

        int processed = 0;
        int errors = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(),
                    SequenceFile.Writer.file(outputPath),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(DonationWritable.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE, new DefaultCodec())
            );
            for (String line = br.readLine();line != null;line = br.readLine()) {
                /*if (processed == 0) {
                    processed++;
                    continue;
                }*/
                try {
                DonationWritable donation = new DonationWritable();
                donation.parseLine(line);
                System.out.println(donation);
                Text key = new Text(donation.donationId);
                writer.append(key,donation);
                    writer.hflush();
                } catch (Exception e) {
                    e.printStackTrace();
                    errors++;
                }
                processed++;
                if (processed%1000 ==0) {
                    System.out.println(String.format("%d thousand lines processed", processed / 1000));
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Number of lines processed:"+processed);
            System.out.println("Number of errors"+errors);
            System.out.printf("took %d ms.\n", System.currentTimeMillis()-start);
        }
    }
}
