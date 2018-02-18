package edu.bu.cs755;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task2 {

    public static class GetMedallionErrors extends Mapper<Object, Text, Text, DoubleWritable> {

        // Set the variable which will be the value in the output map
        private final static DoubleWritable one = new DoubleWritable(1);
        private final static DoubleWritable two = new DoubleWritable(2);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            for (String str : fields
                    ) {
                System.out.println(str + "\n");
            }
            System.out.println(fields.length);
            // only process records with exactly 17 fields, thus discarding malformed records
            if  (fields.length == 17)
                // if the record contains GPS errors, set the value to 1
                if (Double.parseDouble(fields[6]) == 0.000000 && Double.parseDouble(fields[7]) == 0.000000 && Double.parseDouble(fields[8]) == 0.000000 && Double.parseDouble(fields[9]) == 0.000000 )
                    context.write(new Text(fields[0]), one);
                // if it does not have errors, set the value to 2
                else
                    context.write(new Text(fields[0]), two);
        }
    }

    public static class ErrRatePercentageReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Store the number of error records
            double errSum = 0;
            // Store the total number of records
            double totalSum = 0;
            for (DoubleWritable val : values) {
                // increment the total number of records
                totalSum += 1;
                if (val.get() == 1)
                    // if the value is 1 (an error record) increment the error sum
                    errSum += 1;
            System.out.println(key);
            System.out.println("ErrSum" + Double.toString(errSum));
            System.out.println("TotalSum" + Double.toString(totalSum));
            System.out.println("ErrPercentage" + Double.toString(errSum/totalSum));
            }
            // set the result to the percentage of error records in the total records for the given medallion number
            result.set(errSum/totalSum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task1");
        job.setJarByClass(Task2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(Task2.GetMedallionErrors.class);
        //job.setCombinerClass(ErrRatePercentageReducer.class);
        job.setReducerClass(ErrRatePercentageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}