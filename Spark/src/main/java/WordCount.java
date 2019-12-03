import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(String[] args) {

        String inputPath = "resources\\faust.txt";
        String outputPath = "output\\count";

        SparkConf conf = new SparkConf().setAppName("Line-Count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> tokens = sc.textFile(inputPath).flatMap(s -> Arrays.asList(s.split("\\W+")).iterator());
        JavaPairRDD<String, Integer> counts =
                tokens.mapToPair(token -> new Tuple2<>(token, 1)).reduceByKey((x, y) -> x + y);

        counts.saveAsTextFile(outputPath);

        List<Tuple2<String, Integer>> results = counts.collect();
        results.forEach(System.out::println);

        System.out.println("Wörter gesamt: " + counts.count());

        sc.close();

    }

}
