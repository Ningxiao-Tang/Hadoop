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

public class ImprovedCT extends Configured implements Tool {
    public static class NeighborMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        public LongWritable node1 = new LongWritable();
        public LongWritable node2 = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\s+");
            if (line.length > 1) {
                long n1 = Long.parseLong(line[0]);
                long n2 = Long.parseLong(line[1]);

                // Ensure the left node index is less than the right node index
                if (n1 < n2) {
                    node1.set(n1);
                    node2.set(n2);
                }
                else {
                    node1.set(n2);
                    node2.set(n1);
                }
                context.write(node1, node2);
            }
        }

    }


    public static class NeighborReducer extends Reducer<LongWritable, LongWritable, Text, Text> {

        public   LongWritable edge = new LongWritable(-1);
        Set<LongWritable> valuesSet = new HashSet(4000000);
        List<LongWritable> uniqueValues = new ArrayList(4000000);

        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//            ArrayList<Long> valuesArr = new ArrayList<>();
//            for (LongWritable node : values) {
//                valuesArr.add(node.get());
//                //generate original edges
//                pair.set(key.get(),node.get());
//                context.write(new Text(pair.toString()), new Text(edge.toString()));
//            }
//            for (int i = 0; i < valuesArr.size(); ++i) {
//                for (int j = i; j < valuesArr.size(); ++j) {
//                    int compare = valuesArr.get(i).compareTo(valuesArr.get(j));
//                    //emit unique edge
//                    if (compare < 0) {
//                        pair.set(valuesArr.get(i),valuesArr.get(j));
//                        //context.write(new Text(valuesArr.get(i).toString() + ',' + valuesArr.get(j).toString()), new Text(key.toString()));
//                    }
//                    else{
//                        pair.set(valuesArr.get(j),valuesArr.get(i));
//                    }
//                        context.write(new Text(pair.toString()), new Text(key.toString()));
//                }
//            }

            // Insert values to a set to remove duplicates
            Iterator<LongWritable> valuesIterator = values.iterator();
            while (valuesIterator.hasNext()) {
                LongWritable node = valuesIterator.next();
                if (!valuesSet.contains(node)) {
                    valuesSet.add(new LongWritable(node.get()));
                    uniqueValues.add(new LongWritable(node.get()));

                    // Emit single values
                    context.write(new Text(key.toString() + "," + node.toString()), new Text(edge.toString()));
                }
            }

            valuesSet.clear();

            // Emit all edge pairs which are connected on the key node
            for (int i = 0; i < uniqueValues.size(); i++) {
                for (int j = i + 1; j < uniqueValues.size(); j++) {
                    if (uniqueValues.get(i).get() < uniqueValues.get(j).get()) {
                        context.write(new Text(uniqueValues.get(i).toString() + "," + uniqueValues.get(j).toString()), new Text(key.toString()));
                    }
                    else {
                        context.write(new Text(uniqueValues.get(j).toString() + "," + uniqueValues.get(i).toString()), new Text(key.toString()));
                    }
                }
            }

            uniqueValues.clear();
        }


    }

    public static class TriangleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable mark = new LongWritable();

        protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] line = values.toString().split("\\s+");
            mark.set(Long.parseLong(line[1]));
            if(line.length > 1) {
                context.write(new Text(line[0]), mark);
            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable edge = new LongWritable(-1);

        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            boolean hasEdge = false;
            for(LongWritable value: values) {
                if(value.get() == edge.get()) {
                    hasEdge = true;
                }
                else {
                    count++;
                }
            }
            if(hasEdge) {
                LongWritable Count = new LongWritable(count);
                context.write(new Text("Count: "), Count);
            }
        }

    }

//    public static class SumMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
//
//        public LongWritable one = new LongWritable(1);
//        public LongWritable count = new LongWritable();
//
//        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//            String[] line = value.toString().split("\t");
//            count.set(Long.parseLong(line[1]));
//            context.write(one, count);
//        }
//    }
    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable value: values) {
                sum += value.get();
            }
            context.write(new Text("Triangle Count"), new LongWritable(sum));
        }

    }



    public int run(String[] args) throws Exception{
        Job job = new Job(getConf());
        job.setJobName("first-mapreduce");
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(ImprovedCT.class);
        job.setMapperClass(ImprovedCT.NeighborMapper.class);
        job.setReducerClass(ImprovedCT.NeighborReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mr1"));


        Job jobTwo = new Job(getConf());
        jobTwo.setJobName("second-mapreduce");
        jobTwo.setMapOutputKeyClass(Text.class);
        jobTwo.setMapOutputValueClass(LongWritable.class);
        jobTwo.setOutputKeyClass(Text.class);
        jobTwo.setOutputValueClass(LongWritable.class);
        jobTwo.setJarByClass(ImprovedCT.class);
        jobTwo.setMapperClass(ImprovedCT.TriangleMapper.class);
        jobTwo.setReducerClass(ImprovedCT.TriangleReducer.class);

        FileInputFormat.addInputPath(jobTwo, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mr1"));
        FileOutputFormat.setOutputPath(jobTwo, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mr2"));


        Job jobThree = new Job(getConf());
        jobThree.setJobName("third-mapreduce");
        //jobThree.setNumReduceTasks(1);
        jobThree.setMapOutputKeyClass(Text.class);
        jobThree.setMapOutputValueClass(LongWritable.class);
        jobThree.setOutputKeyClass(Text.class);
        jobThree.setOutputValueClass(LongWritable.class);
        jobThree.setJarByClass(ImprovedCT.class);
        jobThree.setMapperClass(ImprovedCT.TriangleMapper.class);
        jobThree.setReducerClass(ImprovedCT.SumReducer.class);

        FileInputFormat.addInputPath(jobThree, new Path("/Users/LIM_x/Desktop/562-DB/proj2/Naive/output1/mr2"));
        FileOutputFormat.setOutputPath(jobThree, new Path(args[1]));

        int ret = job.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobTwo.waitForCompletion(true) ? 0 : 1;
        if (ret == 0)
            ret = jobThree.waitForCompletion(true) ? 0 : 1;

        return ret;

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImprovedCT(), args);
        System.exit(res);

    }

}
