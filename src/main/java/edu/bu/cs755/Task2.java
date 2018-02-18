package edu.bu.cs755;

import java.io.IOException;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Task2 {

    public static class GetMedallionErrors extends Mapper<Object, Text, Text, IntWritable>{

        // Set the variable which will be the value in the output map
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            for (String str : fields
                    ) {
                System.out.println(str + "\n");
            }
            System.out.println(fields.length);
            if  (fields.length == 17)
                if (Double.parseDouble(fields[6]) == 0.000000 && Double.parseDouble(fields[7]) == 0.000000 && Double.parseDouble(fields[8]) == 0.000000 && Double.parseDouble(fields[9]) == 0.000000 )
                    context.write(new Text(fields[0]), one);
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task1");
        job.setJarByClass(Task2.class);
        job.setMapperClass(Task2.GetMedallionErrors.class);
        job.setCombinerClass(Task2.IntSumReducer.class);
        job.setReducerClass(Task2.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}