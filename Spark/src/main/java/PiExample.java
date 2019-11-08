import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class PiExample {
    private final static int NUM_SAMPLES = 1000000;

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("Getting-Started").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y <= 1;
        }).count();

        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);

        sc.close();
    }

}