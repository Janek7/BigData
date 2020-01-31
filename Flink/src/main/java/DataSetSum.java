import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class DataSetSum {  // OK

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> amounts = env.fromElements(1, 2, 10, 50);

        int threshold = 5;
        amounts.filter(a -> a > threshold).reduce((input, sum) -> input + sum).print();

    }

}
