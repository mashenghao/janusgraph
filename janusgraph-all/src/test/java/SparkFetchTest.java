import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 测试一个rdd 备用两次，获取计算两次，
 * 如果是shuffleRDD，则会去executor上拉取上次MapTask的两次shufflewrite的数据
 *
 * @author mahao
 * @date 2022/09/06
 */
public class SparkFetchTest {

    public static void main(String[] args) {
        SparkContext sparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[*]").setAppName("shuffleFetch")
            .set("spark.memory.fraction", "0.4")
            .set("spark.shuffle.spill.numElementsForceSpillThreshold", "3")
        );
        JavaSparkContext sc = new JavaSparkContext(sparkContext);

        JavaPairRDD<String, Integer> rdd = sc
            .parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "0"),2)
            .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, Integer.valueOf(s)))
            .repartition(3);

        long count = rdd.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2).count();
        System.out.println(" =======================   job1 =====================");

        long count2 = rdd.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 - v2).count();
        System.out.println(" =======================   job2 =====================");


    }
}
