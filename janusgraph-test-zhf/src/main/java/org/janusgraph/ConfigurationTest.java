package org.janusgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.GraphFilterAware;
import org.apache.tinkerpop.gremlin.spark.structure.io.InputRDD;

/**
 * Created by Think on 2019/3/25.
 */
public class ConfigurationTest {
    public static void main(String[] args) {
        Configuration hadoopConfiguration = new Configuration();
        hadoopConfiguration.set(Constants.GREMLIN_HADOOP_GRAPH_READER, "org.janusgraph.hadoop.formats.hbase.HBaseInputFormat");
        boolean is1 = InputRDD.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, Object.class));
        boolean is2 = GraphFilterAware.class.isAssignableFrom(hadoopConfiguration.getClass(Constants.GREMLIN_HADOOP_GRAPH_READER, InputFormat.class, InputFormat.class));
        System.out.println(is1+" "+is2);
    }
}
