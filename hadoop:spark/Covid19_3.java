import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.util.*;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.Scanner;
import javax.annotation.processing.Filer;
import org.apache.hadoop.fs.FileSystem;

public class Covid19_3 {
    private static final Log LOG = LogFactory.getLog(Covid19_3.class);

    public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable number = new LongWritable(1);
        private Text word = new Text();

        // The 4 types declared here should match the types that was declared on the top
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArray = value.toString().split(",");
            try {
                word.set(valueArray[1]);
                Long temp = Long.parseLong(valueArray[2]);
                number.set(temp);
                context.write(word, number);
            } catch (NumberFormatException e) {
                LOG.info("This line skipped due to NumberFormatException: " + value.toString());
            }
        }
    }

    // 4 types declared: Type of input key, type of input value, type of output key, type of output value

    // The input types of reduce should match the output type of map
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable total = new LongWritable();
        private LongWritable rate = new LongWritable();

        private  Hashtable<String, Long> joinData = new Hashtable<String, Long>();

        // Notice the that 2nd argument: type of the input value is an Iterable collection of objects
        //  with the same type declared above/as the type of output value from map
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable tmp: values) {
                sum += tmp.get();
            }
            total.set(sum);
            Long population = joinData.get(key.toString());
            if(population != null && population != 0){
                double rateDouble = ((((double)total.get()/(double)population)) * 1000000);
                rate.set((long)rateDouble);
                context.write(key, rate);
            }else{
                LOG.info("This key skipped because population = zero or not found: "+key.toString());
            }
        }

        //Reference: https://buhrmann.github.io/hadoop-distributed-cache.html
        public void setup(Context context){
            try {
                URI[] files = context.getCacheFiles();
                for (URI file : files){
                    String line;
                    FileSystem populationFile = FileSystem.get(context.getConfiguration());
                    Path path = new Path(file.toString());
                    BufferedReader joinReader = new BufferedReader(new InputStreamReader(populationFile.open(path)));
                    while ((line= joinReader.readLine())!=null) {
                        String[] strArray = line.split(",");
                        if(strArray.length == 5){
                            try{
                                Long temp = Long.parseLong(strArray[4]);
                                joinData.put(strArray[1],temp);
                            }catch(NumberFormatException x){
                                LOG.info("skipping this line due to NumberFormatException: "+line);
                            }
                        }else{
                            LOG.info("skipping this line due to extra comma: "+line);
                        }
                    }
                }
            }catch (Exception e) {
                LOG.info("ERROR in SETUP");
            }
        }
    }


    public static void main(String[] args)  throws Exception {
        Configuration conf = new Configuration();
        Job myjob = Job.getInstance(conf, "my coronavirus count 3");
        myjob.setJarByClass(Covid19_3.class);
        myjob.setMapperClass(MyMapper.class);
        myjob.setReducerClass(MyReducer.class);
        myjob.setOutputKeyClass(Text.class);
        myjob.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(myjob, new Path(args[0]));
        myjob.addCacheFile(new Path(args[1]).toUri());
        FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
        System.exit(myjob.waitForCompletion(true) ? 0 : 1);
    }
}

