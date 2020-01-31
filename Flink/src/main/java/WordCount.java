import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCount {  // FEHLER

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("resources\\faust.txt");

        DataSet<Tuple2<String, Integer>> counts = text.map(l -> l.split("\\W+"))
                .flatMap((String[] tokens, Collector<Tuple2<String, Integer>> out)
                        -> Arrays.stream(tokens).filter(t -> t.length() > 0)  // skip empty strings
                        .forEach(t -> out.collect(new Tuple2<>(t, 1)))  // write one default for each entry to sum later
                ).groupBy(0).sum(1);  // group by first field in Tuple (word), sum second field (number)

        counts.print();
    }

}
