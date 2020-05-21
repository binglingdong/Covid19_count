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
import java.text.SimpleDateFormat;
import java.util.Date;

public class Covid19_2 {
    public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable number = new LongWritable(1);
        private Text word = new Text();

        // The 4 types declared here should match the types that was declared on the top
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
            Configuration conf = context.getConfiguration();
            String startDateString = conf.get("startDate");
            String endDateString = conf.get("endDate");
            String[] valueArray = value.toString().split(",");
            try{
                Date startDate = formatter.parse(startDateString);  //Start date entered
                Date endDate = formatter.parse(endDateString);      //End date entered
                Date thisDate = formatter.parse(valueArray[0]);     //Current date evaluating

                //If the currentDate is <= to endDate and >=startDate. Then it's in the range.
                if((thisDate.before(endDate)||thisDate.equals(endDate)) && (thisDate.after(startDate) || thisDate.equals(startDate))){
                    word.set(valueArray[1]);
                    Long temp = Long.parseLong(valueArray[3]);
                    number.set(temp);
                    context.write(word, number);
                }
                //Else ignore

            }catch(Exception e){

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
        SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = null;
        Date endDate = null;
        try{
            startDate=formatter.parse(args[1]);
            endDate=formatter.parse(args[2]);
            Date date = formatter.parse("2019-12-31");
            Date date2 = formatter.parse("2020-04-08");
            if(startDate.after(endDate))throw new Exception();  //If startdate is after enddate throw exception. If == then do nothing.
            if(startDate.before(date)) throw new Exception();   //If startdate is before limitDate throw exception. If == then do nothing
            if(endDate.after(date2)) throw new Exception();     //If enddate is after limitDate throw exception. If == then do nothing.
        }catch(Exception ex){
            System.out.println("Invalid Input, please enter yyyy-mm-dd formatted dates. Make sure" +
                    " start date is before the end date and the dates are from 2019-12-31 to 2020-04-08");
            System.exit(1);
        }
        conf.set("startDate", formatter.format(startDate));
        conf.set("endDate",  formatter.format(endDate));
        Job myjob = Job.getInstance(conf, "my coronavirus count 2");
        myjob.setJarByClass(Covid19_2.class);
        myjob.setMapperClass(MyMapper.class);
        myjob.setReducerClass(MyReducer.class);
        myjob.setOutputKeyClass(Text.class);
        myjob.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(myjob, new Path(args[0]));
        FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
        System.exit(myjob.waitForCompletion(true) ? 0 : 1);
    }
}

