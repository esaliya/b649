package edu.indiana.cs.b649.statistics.distributed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.DecimalFormat;

public class BasicStatistics {
    private static Text k = new Text("k");

    public static class Map extends Mapper<LongWritable, Text, Text, PartialData> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double v = Double.parseDouble(value.toString());
            context.write(k, new PartialData(v, v, v, v*v, 1));
        }
    }

    public static class Combiner extends Reducer<Text, PartialData, Text, PartialData>{
        @Override
        protected void reduce(
            Text key, Iterable<PartialData> values, Context context)
            throws IOException, InterruptedException {

            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double sum = 0.0;
            double sqSum = 0.0;
            int count=0;
            for (PartialData data : values) {
                min = Math.min(data.partialMin, min);
                max = Math.max(data.partialMax, max);
                sum += data.partialSum;
                sqSum += data.partialSqSum;
                count += data.partialCount;
            }
            context.write(k, new PartialData(min, max, sum, sqSum, count));
        }
    }


    public static class Reduce extends Reducer<Text, PartialData, Text, DoubleWritable> {

        DecimalFormat aveFormat = new DecimalFormat("#.##");
        DecimalFormat stdFormat = new DecimalFormat("#.####");

        @Override
        protected void reduce(Text key, Iterable<PartialData> values, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double sum = 0;
            double sqSum = 0;
            int count = 0;

            double average = 0, std = 0;
            for (PartialData data : values) {
                min = Math.min(data.partialMin, min);
                max = Math.max(data.partialMax, max);
                sum += data.partialSum;
                sqSum += data.partialSqSum;
                count += data.partialCount;
            }

            average = sum / count;
            std = Math.sqrt((sqSum / count) -(average*average));
            context.write(new Text("Min"), new DoubleWritable(min));
            context.write(new Text("Max"), new DoubleWritable(max));
            context.write(new Text("Ave"), new DoubleWritable(average));
            context.write(new Text("Std"), new DoubleWritable(std));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: BasicStatistics <in> <out>");
            System.exit(1);
        }

        Job job = new Job(conf, "stats");
        job.setJarByClass(BasicStatistics.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner
        job.setCombinerClass(Combiner.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(PartialData.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
