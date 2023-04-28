package org.janusgraph.graphdb.tinkerpop;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ReadConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.Test;

import java.util.Iterator;

/**
 * @author: mahao
 * @date: 2021/7/7
 */
public class JanusTest {

    public static void main(String[] args) {
        JanusGraph graph = JanusGraphFactory.open("C:\\work\\janusgraph-bs2\\janusgraph-server\\src\\test\\resources\\janusgraph-hbase-mash.properties");
        GraphTraversal<Vertex, Long> count = graph.traversal().V().count();
        System.out.println("结果值 " + count);
    }

    @Test
    public void demo1() {
        org.apache.commons.configuration.Configuration con = new BaseConfiguration();
        con.setProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
        con.setProperty("storage.backend", "hbase");
        con.setProperty("storage.hostname", "10.100.2.110,10.100.2.120,10.100.2.130");
        con.setProperty("storage.port", "2181");
        con.setProperty("storage.hbase.table", "oracle:experiment_211");
        ReadConfiguration con2 = new CommonsConfiguration(con);
        StandardJanusGraph graph2 = new StandardJanusGraph((new GraphDatabaseConfigurationBuilder()).build(con2));
        GraphTraversal t2 = graph2.traversal().V().out().has("value");
        while (t2.hasNext()) {
            Object next = t2.next();
            System.out.println(next.toString());
        }
        System.out.println("end --- ");
        //graph2.close();
    }

    @Test
    public void demo2() {
        org.apache.commons.configuration.Configuration con = new BaseConfiguration();
        con.setProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
        con.setProperty("storage.backend", "hbase");
        con.setProperty("storage.hostname", "10.100.2.110");
        con.setProperty("storage.port", "2181");
        con.setProperty("storage.hbase.table", "oracle:experiment_211");
        ReadConfiguration con2 = new CommonsConfiguration(con);

        GraphDatabaseConfiguration build = (new GraphDatabaseConfigurationBuilder()).build(con2);
        StandardJanusGraph graph2 = new StandardJanusGraph(build);

        graph2.addVertex("label");

        Iterator<Vertex> vertices1 = graph2.vertices(4096);
        while (vertices1.hasNext()) {
            Vertex vertex = vertices1.next();
            System.out.println(vertex.toString());

            VertexProperty<Object> value = vertex.property("value");
            System.out.println(value.value());
            Iterator<Edge> edges = vertex.edges(Direction.OUT);
            while (edges.hasNext()) {
                Edge edge = edges.next();
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                Iterator<Edge> edges1 = inV.edges(Direction.OUT);
                System.out.println(outV + "--" + inV);
            }

        }

        Iterator<Vertex> vertices = graph2.vertices();
        while (vertices.hasNext()) {
            Vertex next = vertices.next();
            System.out.println(next);

            Iterator<Edge> edges = next.edges(Direction.BOTH);
            while (edges.hasNext()) {
                Edge next1 = edges.next();
                System.out.println(next1);
            }

            VertexProperty<Object> value = next.property("value");
            System.out.println(value.value());
            System.out.println("---------------------------");
        }

    }
}
