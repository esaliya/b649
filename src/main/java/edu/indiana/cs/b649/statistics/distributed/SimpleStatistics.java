package edu.indiana.cs.b649.statistics.distributed;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SimpleStatistics {
    private static Text minK = new Text("mink");
    private static Text maxK = new Text("maxk");
    private static Text sumK = new Text("sumk");
    private static Text sqSumK = new Text("sqsumk");
    private static Text countK = new Text("countk");

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double v = Double.parseDouble(value.toString());
            context.write(minK, new DoubleWritable(v));
            context.write(maxK, new DoubleWritable(v));
            context.write(sumK, new DoubleWritable(v));
            context.write(sqSumK, new DoubleWritable(v*v));
            context.write(countK, new DoubleWritable(1));
        }
    }

    public static class Combiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        static double min = Double.MAX_VALUE;
        static double max = Double.MIN_VALUE;
        static double sum = 0.0;
        static double sqSum = 0.0;
        static int count=0;
        @Override
        protected void reduce(
            Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

            if (minK.equals(key)){
                for (DoubleWritable data : values) {
                    min = Math.min(data.get(), min);
                }
                context.write(minK, new DoubleWritable(min));
            } else if (maxK.equals(key)){
                for (DoubleWritable data : values) {
                    max = Math.max(data.get(), max);
                }
                context.write(maxK, new DoubleWritable(max));
            } else if (sumK.equals(key)){
                for (DoubleWritable data : values) {
                    sum += data.get();
                }
                context.write(sumK, new DoubleWritable(sum));
            } else if (sqSumK.equals(key)){
                for (DoubleWritable data : values) {
                    sqSum += data.get();
                }
                context.write(sqSumK, new DoubleWritable(sqSum));
            } else if (countK.equals(key)){
                for (DoubleWritable data : values) {
                    count += data.get();
                }
                context.write(countK, new DoubleWritable(count));
            }
        }
    }


    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        static double min = Double.MAX_VALUE;
        static double max = Double.MIN_VALUE;
        static double sum = 0.0;
        static double sqSum = 0.0;
        static int count=0;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            if (minK.equals(key)){
                for (DoubleWritable data : values) {
                    min = Math.min(data.get(), min);
                }
            } else if (maxK.equals(key)){
                for (DoubleWritable data : values) {
                    max = Math.max(data.get(), max);
                }
            } else if (sumK.equals(key)){
                for (DoubleWritable data : values) {
                    sum += data.get();
                }
            } else if (sqSumK.equals(key)){
                for (DoubleWritable data : values) {
                    sqSum += data.get();
                }
            } else if (countK.equals(key)){
                for (DoubleWritable data : values) {
                    count += data.get();
                }
            }

        }

        @Override
        protected void cleanup(
            Context context) throws IOException, InterruptedException {
            double average = sum / count;
            double std = Math.sqrt((sqSum / count) -(average*average));
            context.write(new Text("Min"), new DoubleWritable(min));
            context.write(new Text("Max"), new DoubleWritable(max));
            context.write(new Text("Ave"), new DoubleWritable(average));
            context.write(new Text("Std"), new DoubleWritable(std));
            super.cleanup(context);
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
        job.setJarByClass(SimpleStatistics.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner
        job.setCombinerClass(Combiner.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(DoubleWritable.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
