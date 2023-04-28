package org.janusgraph;


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;

/**
 * @author: mahao
 * @date: 2021/4/9
 */
public class janus {
    public static void main(String[] args) {
        org.apache.commons.configuration.Configuration con = new BaseConfiguration();
        con.setProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
        con.setProperty("storage.backend", "hbase");
        con.setProperty("storage.hostname", "dn3,dn4,dn5");
        con.setProperty("storage.hbase.ext.zookeeper.znode.parent", "/hbase2021");
        con.setProperty("storage.port", "2181");
        con.setProperty("storage.hbase.table", "default:product_1");
        ReadConfiguration con2 = new CommonsConfiguration(con);
        StandardJanusGraph graph2 = new StandardJanusGraph((new GraphDatabaseConfigurationBuilder()).build(con2));
        GraphTraversal<Vertex, Vertex> t2 = graph2.traversal().V();
        while (t2.hasNext()) {
            Vertex next = t2.next();
            System.out.println(next.toString());
        }
        System.out.println("end --- ");
        //graph2.close();
    }
}
