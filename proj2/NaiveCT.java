import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class NaiveCT extends Configured implements Tool{
    public static final LongWritable EDGE = new LongWritable(-1);

    public static class neighborMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        //Input key type, Input value type, Output key type, Output value type

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] edges = line.split("\\s+");
            if (edges.length > 1){
                long v = Long.parseLong(edges[0]);
                long Taov = Long.parseLong(edges[1]);
                //if (v < Taov)
                context.write(new LongWritable(v), new LongWritable(Taov));
                //else
                //	context.write(new LongWritable(Taov), new LongWritable(v));
            }

        }
    }

    public static class neighborReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        public void reduce(LongWritable key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
            ArrayList<Long> valuesArr = new ArrayList<>();
            for (LongWritable node : value) {
                valuesArr.add(node.get());
                //generate original edges
                context.write(new Text(key.toString() + ',' + node.toString()), new Text(EDGE.toString()));
            }
            for (int i = 0; i < valuesArr.size(); ++i) {
                for (int j = i; j < valuesArr.size(); ++j) {
                    int compare = valuesArr.get(i).compareTo(valuesArr.get(j));
                    //emit unique edge
                    if (compare < 0) {
                        context.write(new Text(valuesArr.get(i).toString() + ',' + valuesArr.get(j).toString()), new Text(key.toString()));
                    }
                    else
                        context.write(new Text(valuesArr.get(j).toString() + ',' + valuesArr.get(i).toString()), new Text(key.toString()));
                }
            }
        }
    }

    public static class TriangleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\s+");
            if(line.length > 1) {
                context.write(new Text(line[0]), new LongWritable(Long.parseLong(line[1])));

            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public LongWritable one = new LongWritable(1);
        public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
            long count = 0;
            List<LongWritable> cache = new ArrayList<>();
            boolean connected = false;
            for (LongWritable v: value) {
                if (v.get()== EDGE.get()) {
                    connected = true;
                } else {
                    count++;
                    cache.add(v);
                }
            }
            if (connected) {
                context.write(new Text("Count:"), new LongWritable(count));
                if(!cache.isEmpty()){
                    for(LongWritable c : cache) {
                        //String traid = key.charAt(0) + "," + key.charAt(2) + "," + c;
                        context.write(new Text(c.toString()), one);
                        context.write(new Text(String.valueOf(key.charAt(0))), one);
                        context.write(new Text(String.valueOf(key.charAt(2))), one);
                    }
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            if(key.toString().equals("Count:"))
                context.write(key, new LongWritable(sum/3));
            else
                context.write(key, new LongWritable(sum));
        }

    }
    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJobName("first-mapreduce");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(NaiveCT.class);
        job.setMapperClass(neighborMapper.class);
        job.setReducerClass(neighborReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mapreduce1"));


        Job jobTwo = new Job(getConf());
        jobTwo.setJobName("second-mapreduce");
        jobTwo.setMapOutputKeyClass(Text.class);
        jobTwo.setMapOutputValueClass(LongWritable.class);
        jobTwo.setOutputKeyClass(Text.class);
        jobTwo.setOutputValueClass(LongWritable.class);
        jobTwo.setJarByClass(NaiveCT.class);
        jobTwo.setMapperClass(TriangleMapper.class);
        jobTwo.setReducerClass(TriangleReducer.class);

        FileInputFormat.addInputPath(jobTwo, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mapreduce1"));
        FileOutputFormat.setOutputPath(jobTwo, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mapreduce2"));


        Job jobThree = new Job(getConf());
        jobThree.setJobName("third-mapreduce");
        //jobThree.setNumReduceTasks(1);
        jobThree.setMapOutputKeyClass(Text.class);
        jobThree.setMapOutputValueClass(LongWritable.class);
        jobThree.setOutputKeyClass(Text.class);
        jobThree.setOutputValueClass(LongWritable.class);
        jobThree.setJarByClass(NaiveCT.class);
        jobThree.setMapperClass(TriangleMapper.class);
        jobThree.setReducerClass(SumReducer.class);

        FileInputFormat.addInputPath(jobThree, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mapreduce2"));
        FileOutputFormat.setOutputPath(jobThree, new Path(args[1]));

        int ret = job.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobTwo.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobThree.waitForCompletion(true) ? 0 : 1;

        return ret;

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new NaiveCT(), args);
        System.exit(res);

    }

}
