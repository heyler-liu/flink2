import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


/**
 * 从文件中读取
 */
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> d = env.readTextFile("hello.txt");
        try {
            d.print();
        env.readTextFile("input").print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
