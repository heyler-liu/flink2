import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


public class ComplexNestedClass {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration",true);
        DataSet<String> logs = env.readTextFile("input")
                .withParameters(configuration).setParallelism(3);           //设置并行线程数，并行度

        try{
            logs.mapPartition(new MapPartitionFunction<String, Long>() {
                public void mapPartition(Iterable)
            }
            })
        }

        DataSet<Integer> length = logs.map(
                (MapFunction<String,Integer>)s->s.length()
        );

        DataSet<String> subString = logs.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out){
                    for(String s : value.split("")){
                out.collect(s);
        });

        try {
            logs.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
