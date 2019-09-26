import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 从csv文件中读取数据
 */
public class JavaDataFromCsv {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer,String,Integer>> dataSet = env
                .readCsvFile("info.csv")
                .ignoreFirstLine()
                .types(Integer.class,String.class,Integer.class);
        try {
            dataSet.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
