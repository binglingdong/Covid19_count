import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_1 {
    //When world flag is turned off. Which contains no world or international information
    public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable number = new LongWritable(1);
        private Text word = new Text();
        // The 4 types declared here should match the types that was declared on the top
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArray = value.toString().split(",");
            if(valueArray[0].contains("2020")){
                if(!valueArray[1].equalsIgnoreCase("World") && !valueArray[1].equalsIgnoreCase("International")){
                    word.set(valueArray[1]);
                    Long temp = Long.parseLong(valueArray[2]);
                    number.set(temp);
                    context.write(word, number);
                }
            }
        }
    }

    //When world flag is turned on. Which contains all data.
    public static class MyMapper2 extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable number = new LongWritable(1);
        private Text word = new Text();
        // The 4 types declared here should match the types that was declared on the top
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArray = value.toString().split(",");
            if(valueArray[0].contains("2020")){
                word.set(valueArray[1]);
                Long temp = Long.parseLong(valueArray[2]);
                number.set(temp);
                context.write(word, number);
            }
        }
    }


    // 4 types declared: Type of input key, type of input value, type of output key, type of output value
    // The input types of reduce should match the output type of map
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();
        // Notice the that 2nd argument: type of the input value is an Iterable collection of objects
        //  with the same type declared above/as the type of output value from map
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable tmp: values) {
                sum += tmp.get();
            }
            total.set(sum);
            // This write to the final output
            context.write(key, total);
        }
    }


    public static void main(String[] args)  throws Exception {
        Configuration conf = new Configuration();
        Job myjob = Job.getInstance(conf, "my coronavirus count 1");
        myjob.setJarByClass(Covid19_1.class);
        myjob.setReducerClass(MyReducer.class);
        myjob.setOutputKeyClass(Text.class);
        myjob.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(myjob, new Path(args[0]));
        if(Boolean.parseBoolean(args[1])){
            myjob.setMapperClass(MyMapper2.class);  //If world = true
        }else{
            myjob.setMapperClass(MyMapper.class);   //If world = false
        }
        FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
        System.exit(myjob.waitForCompletion(true) ? 0 : 1);
    }
}

