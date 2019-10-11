package org.janusgraph.hadoop.util;

import org.apache.commons.configuration.Configuration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import java.util.Map;

/**
 * @author zhf
 */
public class WriteBackServiceUtil {

    public static void setConfigration(Map configMap, Configuration configration) {
        configMap.put(GraphDatabaseConfiguration.GREMLIN_GRAPH.toStringWithoutRoot(), "org.janusgraph.core.JanusGraphFactory");
        configMap.put(GraphDatabaseConfiguration.STORAGE_BACKEND.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + GraphDatabaseConfiguration.STORAGE_BACKEND.toStringWithoutRoot()));
        configMap.put(GraphDatabaseConfiguration.STORAGE_HOSTS.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + GraphDatabaseConfiguration.STORAGE_HOSTS.toStringWithoutRoot()));
        configMap.put(GraphDatabaseConfiguration.STORAGE_PORT.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + GraphDatabaseConfiguration.STORAGE_PORT.toStringWithoutRoot()));
        configMap.put(HBaseStoreManager.HBASE_TABLE.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + HBaseStoreManager.HBASE_TABLE.toStringWithoutRoot()));
    }
}
