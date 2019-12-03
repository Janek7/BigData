import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCountSingle {

    public static void main(String[] args) {

        String path = "resources\\faust.txt";
        SparkConf conf = new SparkConf().setAppName("Line-Count").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> text = sc.textFile(path);
        long count = text.filter(line -> line.contains("Liebe")).count();

        System.out.println(count + " Zeilen mit 'Liebe' in " + path);
        sc.close();

    }

}
