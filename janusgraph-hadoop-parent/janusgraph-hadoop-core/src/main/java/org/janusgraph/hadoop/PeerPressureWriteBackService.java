package org.janusgraph.hadoop;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.spark.process.computer.payload.ViewOutgoingPayload;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.util.WriteBackServiceUtil;
import scala.Tuple2;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;


/**
 * @author zhf
 */
public class PeerPressureWriteBackService implements VertexProgram.WriteBackService<JavaPairRDD<Object, VertexWritable>, Object>, Serializable {
    private Map<String, Object> configMap;

    @Override
    public void setConfigration(Configuration configration) {
        configMap = new HashMap<>();
        WriteBackServiceUtil.setConfigration(configMap, configration);
    }

    @Override
    public void execute(final JavaPairRDD<Object, VertexWritable> input, VertexProgram<Object> vertexProgram) {
        final String[] vertexComputeKeysArray = VertexProgramHelper.vertexComputeKeysAsArray(vertexProgram.getVertexComputeKeys());
        String propertyName = null;
        String prefixName = null;
        for (String key : vertexComputeKeysArray) {
            if (!PeerPressureVertexProgram.VOTE_STRENGTH.equals(key) && !PeerPressureVertexProgram.LOCAL_VOTE_STRENGTH.equals(key)) {
                if (key.startsWith("Prefix")) {
                    prefixName = key.replaceFirst("Prefix", "");
                } else {
                    propertyName = key;
                }
            }
        }
        final String name = propertyName;
        final String prefix = prefixName;
        if (name != null) {
            final CommonsConfiguration config = new CommonsConfiguration(new MapConfiguration(configMap));
            StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(config));
            JanusGraphManagement management = graph.openManagement();
            RelationType key = management.getRelationType(name);
            if (key == null) {
                PropertyKey propertyKey = management.makePropertyKey(name).dataType(String.class).cardinality(Cardinality.SINGLE).make();
                management.setTTL(propertyKey, Duration.ofDays(30));
                Iterator<JanusGraphIndex> nodeIndex = management
                    .getGraphIndexes(org.apache.tinkerpop.gremlin.structure.Edge.class).iterator();
                JanusGraphIndex index = management.buildIndex("indexEBy" + name, Edge.class).addKey(propertyKey, Mapping.TEXTSTRING.asParameter()).buildMixedIndex("search");
                
            }


            management.commit();
            graph.close();
        }
        int count = (int) input.count();
        int commitSize = 10000;
        int partition = count / commitSize + 1;
        JavaPairRDD<Object, VertexWritable> writableRDD = input.repartition(partition);
        writableRDD = writableRDD.mapPartitionsToPair(partitionIterator -> {
            final CommonsConfiguration config = new CommonsConfiguration(new MapConfiguration(configMap));
            StandardJanusGraph graph = new StandardJanusGraph(new GraphDatabaseConfigurationBuilder().build(config));
            JanusGraphTransaction tx = graph.buildTransaction().enableBatchLoading().start();
            while (partitionIterator.hasNext()) {
                StarGraph.StarVertex vertex = partitionIterator.next()._2().get();
                Vertex v = tx.getVertex(Long.valueOf(vertex.id().toString()));
                v.property(VertexProperty.Cardinality.single, name, prefix + vertex.property(name).value());
            }
            Iterator s = IteratorUtils.map(partitionIterator, tuple -> new Tuple2<>(tuple._2().get().get().id(), null));
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
