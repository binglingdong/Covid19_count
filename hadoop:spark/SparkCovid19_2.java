import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.lang.Iterable;
/* Spark imports */
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import java.text.SimpleDateFormat;

public class SparkCovid19_2  {
    /**
     * args[0]: covid19_full_data.csv
     * args[1]: populations.csv
     * args[2]: Output file path on distributed file system
     */
    public static void main(String[] args){
        String input = args[0];
        String input2 = args[1];
        String output = args[2];
        /* essential to run any spark code */
        SparkConf conf = new SparkConf().setAppName("Spark Covid19_2 Task 3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Hashtable<String, Long> joinData = new Hashtable<String, Long>();

        //Setting up Broadcast Variable
        try {
            JavaRDD<String> populationInput = sc.textFile(input2);
            for(String line:populationInput.collect()){
                String[] strArray = line.split(",");
                if(strArray.length == 5){
                    try{
                        Long temp = Long.parseLong(strArray[4]);
                        joinData.put(strArray[1],temp);
                    }catch(NumberFormatException x){
                        System.out.println("skipping this line due to NumberFormatException: "+line);
                    }
                }else{
                    System.out.println("skipping this line due to extra comma: "+line);
                }
            }
        }catch (Exception e) {
            System.out.println("ERROR in SETUP");
        }
        final Broadcast<Hashtable<String, Long>> broadcastVar = sc.broadcast(joinData);


        /* Mapper */
        JavaRDD<String> dataRDD = sc.textFile(input);
        JavaPairRDD<String, Long> MapResult =
            dataRDD.flatMapToPair(new PairFlatMapFunction<String, String, Long>(){
                public Iterator<Tuple2<String, Long>> call(String value){
                    List<Tuple2<String, Long>> mapResult =
                            new ArrayList<Tuple2<String, Long>>();
                    String[] valueArray = value.split(",");
                    try {
                        Long temp = Long.parseLong(valueArray[2]);
                        mapResult.add(new Tuple2<String, Long>(valueArray[1], temp));
                    } catch (NumberFormatException e) {
                        System.out.println("This line skipped due to NumberFormatException: " + value);
                    }
                    return mapResult.iterator();
                }
            });
        //Reducer
        JavaPairRDD<String, Long> ReduceResult=
            MapResult.reduceByKey(new Function2<Long, Long, Long>(){
            public Long call(Long x, Long y){
                return x+y;
            }
        });
//        System.out.println(broadcastVar.value().toString());
        ReduceResult = ReduceResult.mapToPair((t)->{
            Long population = broadcastVar.value().get(t._1);
            if(population != null && population != 0){
                double rateDouble = ((((double)t._2/(double)population)) * 1000000);
                return new Tuple2<String, Long>(t._1, (long)rateDouble);
            }else{
                System.out.println("This key skipped because population = zero or not found: "+t._1);
                return new Tuple2<String, Long>(t._1,null);
            }
        }).filter(t -> t._2!=null);

        ReduceResult.saveAsTextFile(output);
    }
}
