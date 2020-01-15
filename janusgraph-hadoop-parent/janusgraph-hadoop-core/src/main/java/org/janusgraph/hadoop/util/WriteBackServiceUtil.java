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

    private static String ZK_NODE_PARENT = "janusgraphmr.ioformat.conf.storage.hbase.ext.zookeeper.znode.parent";
    private static String HBASE_RS_KB_PRINCIPAL = "janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.regionserver.kerberos.principal";
    private static String HBASE_MA_KB_PRINCIPAL = "janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.master.kerberos.principal";
    private static String ZK_QUIRUM = "janusgraphmr.ioformat.conf.storage.hbase.ext.zookeeper.quorum";
    private static String HBASE_ZK_CLIENT_PORT = "janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.zookeeper.property.clientPort";
    private static String HADOOP_SECURITY_AUTHENTICATION = "janusgraphmr.ioformat.conf.storage.hbase.ext.hadoop.security.authentication";
    private static String hadoop_security_authorization = "janusgraphmr.ioformat.conf.storage.hbase.ext.hadoop.security.authorization";
    private static String hbase_security_authentication = "janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.security.authentication";
    private static String hbase_security_authorization = "janusgraphmr.ioformat.conf.storage.hbase.ext.hbase.security.authorization";
    private static String java_security_krb5_conf = "java.security.krb5.conf";
    private static String kerberos_principal = "janusgraphmr.ioformat.conf.storage.hbase.ext.kerberos.principal";
    public static void setConfigration(Map configMap, Configuration configration) {
        configMap.put(GraphDatabaseConfiguration.GREMLIN_GRAPH.toStringWithoutRoot(), "org.janusgraph.core.JanusGraphFactory");
        configMap.put(GraphDatabaseConfiguration.STORAGE_BACKEND.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + GraphDatabaseConfiguration.STORAGE_BACKEND.toStringWithoutRoot()));
        configMap.put(GraphDatabaseConfiguration.STORAGE_HOSTS.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + GraphDatabaseConfiguration.STORAGE_HOSTS.toStringWithoutRoot()));
        configMap.put(GraphDatabaseConfiguration.STORAGE_PORT.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + GraphDatabaseConfiguration.STORAGE_PORT.toStringWithoutRoot()));
        configMap.put(HBaseStoreManager.HBASE_TABLE.toStringWithoutRoot(), configration.getProperty(JanusGraphHadoopConfiguration.GRAPH_CONFIG_KEYS + "." + HBaseStoreManager.HBASE_TABLE.toStringWithoutRoot()));
        if(configration.containsKey(ZK_NODE_PARENT)){
            configMap.put("storage.hbase.ext.zookeeper.znode.parent", configration.getProperty(ZK_NODE_PARENT));
        }
        if(configration.containsKey(HBASE_RS_KB_PRINCIPAL)){
            configMap.put("storage.hbase.ext.hbase.regionserver.kerberos.principal",configration.getProperty(HBASE_RS_KB_PRINCIPAL));
        }
        if(configration.containsKey(HBASE_MA_KB_PRINCIPAL)){
            configMap.put("storage.hbase.ext.hbase.master.kerberos.principal",configration.getProperty(HBASE_MA_KB_PRINCIPAL));
        }
        if(configration.containsKey(ZK_QUIRUM)){
            configMap.put("storage.hbase.ext.hbase.zookeeper.quorum",configration.getProperty(ZK_QUIRUM));
        }
        if(configration.containsKey(HBASE_ZK_CLIENT_PORT)){
            configMap.put("storage.hbase.ext.hbase.zookeeper.property.clientPort",configration.getProperty(HBASE_ZK_CLIENT_PORT));
        }
        if(configration.containsKey(HADOOP_SECURITY_AUTHENTICATION)){
            configMap.put("storage.hbase.ext.hadoop.security.authentication",configration.getProperty(HADOOP_SECURITY_AUTHENTICATION));
        }
        if(configration.containsKey(hadoop_security_authorization)){
            configMap.put("storage.hbase.ext.hadoop.security.authorization",configration.getProperty(hadoop_security_authorization));
        }
        if(configration.containsKey(hbase_security_authentication)){
            configMap.put("storage.hbase.ext.hbase.security.authentication",configration.getProperty(hbase_security_authentication));
        }if(configration.containsKey(hbase_security_authorization)){
            configMap.put("storage.hbase.ext.hbase.security.authorization",configration.getProperty(hbase_security_authorization));
        }
        if(configration.containsKey(java_security_krb5_conf)){
            configMap.put("java_security_krb5_conf",configration.getProperty(java_security_krb5_conf));
        }
        if(configration.containsKey(kerberos_principal)){
            configMap.put("storage.hbase.ext.kerberos.principal",configration.getProperty(kerberos_principal));
        }
    }
}
