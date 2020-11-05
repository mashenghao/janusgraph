package org.janusgraph.hadoop;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.hadoop.util.WriteBackServiceUtil;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Think
 **/
public class EdgeCountWriteBackService implements VertexProgram.WriteBackService<JavaPairRDD<Object, VertexWritable>, Object>, Serializable {

    private Map<String, Object> configMap;
    private static final String PROPERTY_NAME = "NECount";

    @Override
    public void setConfigration(Configuration configration) {
        configMap = new HashMap<>();
        WriteBackServiceUtil.setConfigration(configMap,configration);
    }

    @Override
    public void execute(JavaPairRDD<Object, VertexWritable> input, VertexProgram<Object> vertexProgram) {
        int count = (int) input.count();
        int commitSize = 10000;
        int partition = count / commitSize + 1;
        JavaPairRDD<Object, VertexWritable> writableRDD = input.repartition(partition);
        writableRDD = writableRDD.mapPartitionsToPair(partitionIterator -> {
            final CommonsConfiguration config = new CommonsConfiguration(new MapConfiguration(configMap));
            StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(config));
            JanusGraphTransaction tx = graph.buildTransaction().enableBatchLoading().start();
            while(partitionIterator.hasNext()){
                StarGraph.StarVertex vertex = partitionIterator.next()._2().get();
                Vertex v = tx.getVertex(Long.valueOf(vertex.id().toString()));
                v.property(VertexProperty.Cardinality.single, PROPERTY_NAME, vertex.property(PROPERTY_NAME).value());
            }
            Iterator s = IteratorUtils.map(partitionIterator, tuple -> new Tuple2<>(tuple._2().get().get().id(),null));
            tx.commit();
            tx.close();
            graph.close();
            return s;
        });
        writableRDD.count();
    }

    @Override
    public Map<String, Object> getGraphConfig() {
        return configMap;
    }
}
