import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountTest {

    public static void main(String[] args) {

        String inputPath = "resources\\faust.txt";
        //String outputPath = "output\\count" + new SimpleDateFormat("yyyy-mm-dd|hh:mm:ss").format(Calendar.getInstance().getTime());

        SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> tokens = sc.textFile(inputPath)
                .flatMap(s -> Arrays.asList(s.split("\\W+")).iterator());

        JavaPairRDD<String, Integer> counts = tokens.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((x, y) -> x + y);

        counts.collect().forEach(System.out::println);

        sc.close();

    }

}
