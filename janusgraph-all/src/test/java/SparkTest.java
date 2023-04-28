import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author mahao
 * @date 2022/09/01
 */
public class SparkTest {

    public static void main(String[] args) {
        SparkContext sparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("readLoaddata")
            .set("spark.memory.fraction", "0.4")
            .set("spark.shuffle.spill.numElementsForceSpillThreshold", "3")
        );
        JavaSparkContext sc = new JavaSparkContext(sparkContext);
        long count = sc.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "0"), 1)
            .mapToPair((PairFunction<String, String, byte[]>) integer -> {
                if ("6".equals(integer)){
                    Thread.sleep(10000000);
                }
                return new Tuple2<>(integer, new byte[1024 * 1024 * 200]);
            })
           .reduceByKey((Function2<byte[], byte[], byte[]>) (v1, v2) -> new byte[1024 * 1024])
            .count();
        System.out.println(count);


    }
}
