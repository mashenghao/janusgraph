import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputFormatRDD;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mahao
 * @date 2022/08/21
 */
public class QueryVertex {

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
//        Configuration conf = new BaseConfiguration();
//        conf.addProperty("storage.backend", "hbase");
//        conf.addProperty("storage.hostname", "cs3");
//        conf.addProperty("storage.port", "2181");
//        conf.addProperty("storage.hbase.table", "product_2");
//        StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(conf);
        SparkContext sparkContext = SparkContext.getOrCreate(new SparkConf().setMaster("local[1]").setAppName("readLoaddata")
//            .set("spark.memory.fraction", "0.4")
                .set("spark.shuffle.spill.numElementsForceSpillThreshold", "200")
        );
        JavaSparkContext sc = new JavaSparkContext(sparkContext);
        HadoopConfiguration graphComputerConfiguration = new HadoopConfiguration();
        graphComputerConfiguration.setProperty("janusgraphmr.ioformat.conf.storage.hostname", "cs3");
        graphComputerConfiguration.setProperty("janusgraphmr.ioformat.conf.storage.port", "2181");
        graphComputerConfiguration.setProperty("janusgraphmr.ioformat.conf.storage.hbase.table", "janus1");
        graphComputerConfiguration.setProperty("janusgraphmr.ioformat.conf.storage.backend", "hbase");
        graphComputerConfiguration.setProperty("gremlin.hadoop.graphReader", "org.janusgraph.hadoop.formats.hbase.HBaseInputFormat");
        AtomicLong atomicLong = new AtomicLong(0);

        JavaPairRDD<Object, VertexWritable> rdd = InputFormatRDD.class.newInstance().readGraphRDD(graphComputerConfiguration, sc);


//        JavaPairRDD<Object, VertexWritable> pairRDD = rdd.mapToPair(e -> {
//                if (atomicLong.incrementAndGet() % 1000 == 0) {
//                    System.out.println(TaskContext.get().taskAttemptId() + " 读取数据数" + atomicLong.incrementAndGet());
//                }
//                return new Tuple2<>(e._1, e._2);
//            })
//            .reduceByKey((Function2<VertexWritable, VertexWritable, VertexWritable>) (v1, v2) -> v1);

        long l = System.currentTimeMillis();
        System.out.println("==================" + rdd.count());
        System.out.println("=======" + (System.currentTimeMillis() - l));

    }
}
