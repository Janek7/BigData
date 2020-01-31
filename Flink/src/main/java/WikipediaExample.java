import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import scala.Tuple2;

public class WikipediaExample {  //t.f0 attribut?

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<WikipediaEditEvent> edits = env.addSource(new WikipediaEditsSource());
        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(WikipediaEditEvent::getUser);

        DataStream<Tuple2<String, String>> results = keyedEdits.map(event -> new Tuple2<String, String>(event.getUser(), event.getSummary()))
                .keyBy(t -> t._1).timeWindow(Time.minutes(1)).reduce((t1, t2) -> new Tuple2<String, String>(t1._1, t1._2 + " +++ " + t2._2));

        results.print();

    }

}
