import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class BasicDataStreamInt {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> amounts = env.fromElements(1, 5, 23, 345, 546, 76, 3434, 42, 3445, 345);
        int threshold = 30;

        amounts
                .filter(a -> a > threshold)
                .print();

        // ab hier wird das Stream-Processing gestartet
        env.execute();

        System.out.println("Ende von main erreicht.");
    }

}