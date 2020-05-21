import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.lang.Iterable;
import java.util.Iterator;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SparkCovid19_1  {
    /**
     * args[0]: Input file path on distributed file system
     * args[1]: Start Date
     * args[2]: End Date
     * args[3]: Output file path on distributed file system
     */
    public static void main(String[] args){
        String input = args[0];
        String output = args[3];
        /* essential to run any spark code */
        SparkConf conf = new SparkConf().setAppName("Spark Covid19_1 Task 2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*Code from Covid19_2.java*/
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

        final String startDateString = args[1];
        final String endDateString = args[2];

        /* load input data to RDD */
        JavaRDD<String> dataRDD = sc.textFile(input);
        JavaPairRDD<String, Long> counts =
            dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Long>(){
                public Iterator<Tuple2<String, Long>> call(String value){
                    List<Tuple2<String, Long>> mapResult =
                            new ArrayList<Tuple2<String, Long>>();

                    SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
                    String[] valueArray = value.split(",");
                    try{
                        Date startDate = formatter.parse(startDateString);  //Start date entered
                        Date endDate = formatter.parse(endDateString);      //End date entered
                        Date thisDate = formatter.parse(valueArray[0]);     //Current date evaluating

                        //If the currentDate is <= to endDate and >=startDate. Then it's in the range.
                        if((thisDate.before(endDate)||thisDate.equals(endDate)) && (thisDate.after(startDate) || thisDate.equals(startDate))){
                            Long temp = Long.parseLong(valueArray[3]);
                            mapResult.add(new Tuple2<String, Long>(valueArray[1], temp));
                        }
                        //Else ignore
                    }catch(Exception e){
                        System.out.println("Error parsing the dates");
                    }
                    return mapResult.iterator();
                }
            }).reduceByKey(new Function2<Long, Long, Long>(){
                public Long call(Long x, Long y){
                    return x+y;
                }
            });
        counts.saveAsTextFile(output);

    }
}
