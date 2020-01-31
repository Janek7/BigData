package exercises;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.fromElements("Hallo", "welt", "Hallo", "wie");
        dataStream.flatMap((String word, Collector<Tuple2<String, Integer>> out)
                -> out.collect(new Tuple2<>(word, 1))).keyBy(0).sum(1).print();

        env.execute();

    }

}
