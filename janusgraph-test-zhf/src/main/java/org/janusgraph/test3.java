package org.janusgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

import java.io.File;
import java.io.IOException;

/**
 * Created by Think on 2019/4/15.
 */
public class test3 {
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static final String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.hadoop.com";

    private static Configuration conf = null;
    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            String userdir = "D:\\git\\janusgraph\\janusgraph-test-zhf\\src\\main\\resources\\conf\\";
//            String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            userName = "developuser";
            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";

            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY,
                ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
//        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        String userdir = "D:\\git\\janusgraph\\janusgraph-test-zhf\\src\\main\\resources\\conf\\";
        conf.addResource(new Path(userdir + "core-site.xml"));
        conf.addResource(new Path(userdir + "hdfs-site.xml"));
        conf.addResource(new Path(userdir + "hbase-site.xml"));

    }

    public static void main(String[] args) throws Exception {
        Graph graph = null;
        GraphTraversalSource g = null;
        JanusGraphTransaction graphTransaction = null;
        try {
            init();
            login();
            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-kerberos.properties");
            g = graph.traversal();
            System.out.println(g.V().count().next());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (graph != null) {
                graph.close();
            }
            if (g != null) {
                g.close();
            }
            if (graphTransaction != null) {
                graphTransaction.close();
            }
        }
    }
}
