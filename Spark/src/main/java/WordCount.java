import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {

        String inputPath = "resources\\faust.txt";
        //String outputPath = "output\\count" + new SimpleDateFormat("yyyy-mm-dd|hh:mm:ss").format(Calendar.getInstance().getTime());

        SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> tokens = sc.textFile(inputPath)
                .flatMap(s -> Arrays.asList(s.split("\\W+")).iterator());
        JavaPairRDD<String, Integer> counts =
                tokens.mapToPair(token -> new Tuple2<>(token, 1)).reduceByKey((x, y) -> + y);
        //counts.saveAsTextFile(outputPath);

        List<Tuple2<String, Integer>> results = counts.collect();
        results.forEach(System.out::println);

        System.out.println("Wörter gesamt: " + counts.count());

        sc.close();


    }

}
