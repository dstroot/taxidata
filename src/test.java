import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class test {

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");
            if (words.length > 10 && !words[0].equals("VendorID")) {
                word.set(words[5] + "-" + words[6]);
                context.write(word, new DoubleWritable(Double.parseDouble(words[16])));
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        private HashMap<String, Double> sortedList = new HashMap<String, Double>();


        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum = value.get();
            }
            result.set(sum);
            sortedList.put(key.toString(), result.get());
            //context.write(key, result);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            HashMap<Double, String> flippedList = new HashMap<Double, String>();
            for (String key : sortedList.keySet()) {
                flippedList.put(sortedList.get(key), key);
            }

            ArrayList<Double> list = new ArrayList<Double>(flippedList.keySet());
            Collections.sort(list);
            for (int i = list.size() - 1; i > list.size() - 11 && i > 0; i--) {
                context.write(new Text(flippedList.get(list.get(i))), new DoubleWritable(list.get(i)));
            }

            for (String key : sortedList.keySet()) {
                //context.write(new Text(key), new IntWritable(sortedList.get(key)));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}