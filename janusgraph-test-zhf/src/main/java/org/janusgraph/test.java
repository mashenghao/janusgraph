package org.janusgraph;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.drop.DropVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.udf.UdfFunctions;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.udf.CustomFunctions;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.graphdb.database.management.ModifierType;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.groovy.jsr223.dsl.credential.__.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gte;
import static org.apache.tinkerpop.gremlin.process.traversal.P.within;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.label;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.spark_project.jetty.util.BlockingArrayQueue;

public class test {

    @Test
    public void testtttt(){
        System.out.println("2048005734624".compareTo("1331224"));
    }

    @Test
    public void addNative() throws Exception {
        JanusGraph graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-test.properties");
        Vertex v1 = graph.addVertex("person");
        v1.property("name", "cat", "age", "11");
        Vertex v2 = graph.addVertex(T.label, "software", "name", "blueprints");
        v2.addEdge("like", v1, "create", "2009");

        graph.tx().commit();
        graph.close();

    }

    @Test
    public void add2() throws Exception {
        JanusGraph graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-tmptest2.properties");
        GraphTraversalSource g = graph.traversal();
        createSchema11(graph);
        g = graph.traversal();
        createVertex9(g);
    }

    static void setHadoopConf() {
        ClassLoader classLoader = test.class.getClassLoader();
        Configuration hadoopConfig = new Configuration();
        System.out.println(System.getProperty("user.dir"));
        System.out.println(classLoader.getResource(""));
        hadoopConfig.addResource(classLoader.getResource("conf1/core-site.xml"));
        hadoopConfig.addResource(classLoader.getResource("conf1/hdfs-site.xml"));
        hadoopConfig.addResource(classLoader.getResource("conf1/mapred-site.xml"));
        hadoopConfig.addResource(classLoader.getResource("conf1/yarn-site.xml"));
    }

    @Test
    public void test1() throws Exception {
        Graph graph = null;
        GraphTraversalSource g = null;
        JanusGraphTransaction graphTransaction = null;
        try {
            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-tmptest1.properties");
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-test.properties");
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-graphExam.properties");
//            graph = GraphFactory.open("src/main/resources/read-hbase-tmptest2.properties");
//            graph = JanusGraphFactory.open("src/main/resources/JanusGraph-configurationmanagement1.properties");
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-cassandra-es-tmp.properties");
            g = graph.traversal();
            System.out.println(g.V().hasId(4120).next());
            System.out.println(g.V().has("node_id","11").valueMap().next());
            System.out.println(g.V().has("node_id",11).hasId(4120).next());
        } catch (Exception e) {
            throw e;
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

    @Test
    public void createGraph(){
        JanusGraph graph = null;
        GraphTraversalSource g = null;
        try{
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-tmptest2.properties");
//            createSchema10(graph);
//            System.out.println(TypeDefinitionCategory.CONSISTENCY_LEVEL.name());
            System.out.println(ModifierType.CONSISTENCY.name());
        }catch (Throwable e){
            throw e;
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println(net.jpountz.lz4.LZ4BlockInputStream.class.getProtectionDomain().getCodeSource().getLocation());
//        JanusGraph graph = null;
        Graph graph = null;
        GraphTraversalSource g = null;
        JanusGraphTransaction graphTransaction = null;
        try {
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-tmptest2.properties");
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-test.properties");
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-hbase-es-graphExam.properties");
//            graph = GraphFactory.open("src/main/resources/read-hbase-tmptest5.properties");
            graph = GraphFactory.open("src/main/resources/read-hbase-product19.properties");
//            graph = JanusGraphFactory.open("src/main/resources/JanusGraph-configurationmanagement1.properties");
//            graph = JanusGraphFactory.open("src/main/resources/janusgraph-cassandra-es-tmp.properties");
//            createSchema2(graph);
//            createSchema6(graph);

//            createSchema2(graph);
//            changeTTL(graph);
//            getTTL(graph);
//            createSchema5(graph);
//            g = graph.traversal();
//            Vertex v = g.V().next();
//            List<Object> bs = new ArrayList<>();
//            v.values("user_label").forEachRemaining(c -> bs.add(c));
//            bs.toString();
//            g.V().valueMap(true).next();
//            createVertex(g);
//            createVertex8(g)
//            concurrentInsert(graph);
//            createVertex6(g);
//            createVertex3(g);
//            g = graph.traversal().withComputer();
            g = graph.traversal().withComputer(SparkGraphComputer.class);
//            System.out.println(g.withComputer(Computer.compute(SparkGraphComputer.class).workers(20)).V().as("a").hasLabel("T1").both().values("value").as("b").select("a").group().by("value").by(select("b").filter(is(P.eq("4"))).count()).next(1000));
//            System.out.println(g.withComputer(Computer.compute(SparkGraphComputer.class).edges(bothE("E1")).properties("").workers(997))
//                .V().as("A").out("E1").out("E1").as("B").group().by(select("A")).by(select("B").count())
//                .unfold().next(100));
//                .unfoldM().where(select(Column.values).is(P.gt(2))).select(Column.keys).as("A1").outE().inV().outE().inV().outE().inV().path().count().next(1000000000));
//            System.out.println(g.withComputer(Computer.compute(SparkGraphComputer.class).vertices(hasLabel("客户")).edges(bothE("担保")).properties("担保金额").workers(20))
//                .V().as("a").out().as("b").group().by(select("a")).by(select("b").count()).unfoldM().select(Column.keys).out().out().next(100));
//                .by(fold().match(__.as("p").unfold().inE().values("担保金额").sum().as("inSum"),
//                    __.as("p").unfold().outE().values("担保金额").sum().math("_*0.8").as("outSum"),
//                    __.as("p").unfold().inE().count().as("inCount"),
//                    __.as("p").unfold().outE().count().math("_*1").as("outCount"),
//                    __.as("p").unfold().out().dedup().count().as("outCust"))
//                    .select("inSum","outSum","inCount","outCount","outCust"))
//                .unfold().filter(select(Column.values).select("inSum").is(P.gt(1000000)))
//                .filter(select(Column.values).where("outSum",P.gt("inSum")))
//                .filter(select(Column.values).where("inCount",P.gt("outCount")))
//                .filter(select(Column.values).select("outCust").is(P.gte(1))).select(Column.keys).count().next(100));
//                .V().count().next());
            graph.compute(SparkGraphComputer.class).edges(bothE()).properties("").program(PeerPressureVertexProgram.build().property("community1").prefix("").edges(bothE()).maxIterations(30).create()).submit().get();
        } catch (Exception e) {
            throw e;
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

    public static void concurrentInsert(JanusGraph graph) {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(8, 16, 0, TimeUnit.MILLISECONDS, new BlockingArrayQueue<Runnable>(100), Executors.defaultThreadFactory(), new CustomRejectedExecutionHandler());
//        ThreadPoolExecutor pool = Executors.newFixedThreadPool(8);
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            tasks.add(new InsertTask(graph, i));
        }
        try {
            List<Future<Void>> futures = pool.invokeAll(tasks);
            checkExeception(futures);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static <T> void checkExeception(List<Future<T>> futures) {
        for (Future<T> f : futures) {
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void changeTTL(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        Iterable<EdgeLabel> labels = mgmt.getRelationTypes(EdgeLabel.class);
        labels.forEach(label -> mgmt.setTTL(label, Duration.ofSeconds(10)));
        Iterable<PropertyKey> properties = mgmt.getRelationTypes(PropertyKey.class);
        properties.forEach(label -> mgmt.setTTL(label, Duration.ofSeconds(10)));
        mgmt.commit();
    }

    public static void getTTL(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        Iterable<EdgeLabel> labels = mgmt.getRelationTypes(EdgeLabel.class);
        labels.forEach(label -> System.out.println(mgmt.getTTL(label)));
        Iterable<PropertyKey> properties = mgmt.getRelationTypes(PropertyKey.class);
        properties.forEach(propertyKey -> System.out.println(mgmt.getTTL(propertyKey)));
    }

    public static void createSchema(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("user").make();
        mgmt.makeVertexLabel("phone").make();
        mgmt.makeEdgeLabel("Knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UsePhone").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("transtime").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("gremlin.traversalVertexProgram.haltedTraversers").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("gremlin.traversalVertexProgram.activeTraversers").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("cluster").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("gremlin.peerPressureVertexProgram.voteStrength").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
        mgmt.commit();
    }

    public static void createSchema2(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("card").make();
        mgmt.makeVertexLabel("merchant").make();
        mgmt.makeVertexLabel("phone").make();
        mgmt.makeEdgeLabel("Pay").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UsePhone").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("transtime").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("gremlin.traversalVertexProgram.haltedTraversers").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("gremlin.traversalVertexProgram.activeTraversers").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("cluster").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("gremlin.peerPressureVertexProgram.voteStrength").dataType(Object.class).cardinality(Cardinality.SINGLE).make();
        mgmt.commit();
    }

    public static void createSchema3(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("merchant").make();
        mgmt.makeVertexLabel("card").make();
        EdgeLabel pay = mgmt.makeEdgeLabel("Pay").multiplicity(Multiplicity.MULTI).make();
//        mgmt.setTTL(pay, Duration.ofSeconds(10));
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey value = mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey transtime = mgmt.makePropertyKey("transtime").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("byValue", Vertex.class).addKey(value).buildCompositeIndex();
        mgmt.buildIndex("byTranstime", Edge.class).addKey(transtime).buildCompositeIndex();
        mgmt.commit();
    }

    public static void createSchema4(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("frameno").make();
        mgmt.makeVertexLabel("lazb_registno").make();
        EdgeLabel pay = mgmt.makeEdgeLabel("sgglc").multiplicity(Multiplicity.MULTI).make();
//        mgmt.setTTL(pay, Duration.ofSeconds(10));
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey value = mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey transtime = mgmt.makePropertyKey("transtime").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("byValue", Vertex.class).addKey(value).buildCompositeIndex();
        mgmt.buildIndex("byTranstime", Edge.class).addKey(transtime).buildCompositeIndex();
        mgmt.commit();
    }

    public static void createSchema5(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("frameno").make();
        mgmt.makeVertexLabel("lazb_registno").make();
        mgmt.makeVertexLabel("lpb_payeebankaccountname").make();
        mgmt.makeVertexLabel("bdx_insurecode").make();
        EdgeLabel pay1 = mgmt.makeEdgeLabel("sgglc").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel pay2 = mgmt.makeEdgeLabel("sgskr").multiplicity(Multiplicity.MULTI).make();
        EdgeLabel pay3 = mgmt.makeEdgeLabel("cbbxr").multiplicity(Multiplicity.MULTI).make();
//        mgmt.setTTL(pay, Duration.ofSeconds(10));
        PropertyKey name = mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey value = mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey transtime = mgmt.makePropertyKey("transtime").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.setTTL(value, Duration.ofSeconds(10));
        mgmt.buildIndex("byValue", Vertex.class).addKey(value).buildCompositeIndex();
        mgmt.buildIndex("byTranstime", Edge.class).addKey(transtime).buildCompositeIndex();
        mgmt.commit();
    }

    public static void createSchema6(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("people").make();
        EdgeLabel trade = mgmt.makeEdgeLabel("trade").multiplicity(Multiplicity.MULTI).make();
        PropertyKey id = mgmt.makePropertyKey("id").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey amt = mgmt.makePropertyKey("amt").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        JanusGraphIndex indexEvalue = mgmt.buildIndex("byEAmt", Edge.class).addKey(amt).unique().buildCompositeIndex();
        JanusGraphIndex indexVid = mgmt.buildIndex("byVId", Vertex.class).addKey(id).unique().buildCompositeIndex();
//        RelationTypeIndex indexTradeByAmt = mgmt.buildEdgeIndex(trade, "tradeByAmt", Direction.BOTH, Order.desc, amt);
        mgmt.setConsistency(id, ConsistencyModifier.LOCK);
        mgmt.setConsistency(amt, ConsistencyModifier.LOCK);
        mgmt.setConsistency(indexEvalue, ConsistencyModifier.LOCK);
        mgmt.setConsistency(indexVid, ConsistencyModifier.LOCK);

//        mgmt.setConsistency(indexTradeByAmt, ConsistencyModifier.LOCK);
        mgmt.commit();
//        GraphTraversalSource g = graph.traversal();
//        g.V().addV("people").property("id","1").next();
//        g.V().addV("people").property("id","2").next();
//        g.E().addE("trade").property("amt","1").next();
//        g.tx().commit();
//        try {
//            g.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    public static void createSchema8(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("people").make();
        mgmt.makeVertexLabel("phone").make();
        mgmt.makeEdgeLabel("know").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UsePhone").multiplicity(Multiplicity.MULTI).make();
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("phoneNum").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("phoneAddr").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.commit();
    }

    public static void createSchema9(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("Apply").make();
        mgmt.makeVertexLabel("Company").make();
        mgmt.makeVertexLabel("Device").make();
        mgmt.makeVertexLabel("Email").make();
        mgmt.makeVertexLabel("IdCard").make();
        mgmt.makeVertexLabel("Phone").make();
        mgmt.makeVertexLabel("Promoter").make();
        mgmt.makeVertexLabel("PromoterApply").make();
        mgmt.makeEdgeLabel("UseCompany").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UseDevice").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UseEmail").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UseIdCard").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UsePhone").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UsePromoter").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("UsePromoterApply").multiplicity(Multiplicity.MULTI).make();
        PropertyKey value = mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey node_id = mgmt.makePropertyKey("node_id").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey evalue = mgmt.makePropertyKey("evalue").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("amt").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("info").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.buildIndex("byValue", Vertex.class).addKey(value).buildCompositeIndex();
        mgmt.buildIndex("byNodeId", Vertex.class).addKey(node_id).buildCompositeIndex();
        JanusGraphIndex indexEvalue = mgmt.buildIndex("byEValue", Edge.class).addKey(evalue).buildCompositeIndex();
        mgmt.setConsistency(evalue, ConsistencyModifier.LOCK);
        mgmt.setConsistency(indexEvalue, ConsistencyModifier.LOCK);
        mgmt.commit();
    }

    public static void createSchema11(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("T1").make();
        mgmt.makeEdgeLabel("E1").multiplicity(Multiplicity.MULTI).make();
        PropertyKey value = mgmt.makePropertyKey("value").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.commit();
    }

    public static void createSchema10(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makeVertexLabel("people").make();
        EdgeLabel trade = mgmt.makeEdgeLabel("trade").multiplicity(Multiplicity.MULTI).make();
        PropertyKey id = mgmt.makePropertyKey("id").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey amt = mgmt.makePropertyKey("amt").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        PropertyKey EdgeCount = mgmt.makePropertyKey("EdgeCount").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        JanusGraphIndex byVid = mgmt.buildIndex("byVid", Vertex.class).addKey(id).unique().buildCompositeIndex();
        mgmt.buildIndex("byEamt", Edge.class).addKey(amt).buildCompositeIndex();
        mgmt.setConsistency(EdgeCount, ConsistencyModifier.LOCK);
        mgmt.setConsistency(id, ConsistencyModifier.LOCK);
        mgmt.setConsistency(byVid, ConsistencyModifier.LOCK);
        mgmt.commit();
    }

    public static void createVertex10(GraphTraversalSource g){

    }
    public static void createVertex9(GraphTraversalSource g) {
        Vertex T1 = g.addV("T1").property("value","1").next();
        Vertex T2 = g.addV("T1").property("value","2").next();
        Vertex T3 = g.addV("T1").property("value","3").next();
        Vertex T4 = g.addV("T1").property("value","4").next();
        Vertex T5 = g.addV("T1").property("value","5").next();
        Vertex T6 = g.addV("T1").property("value","6").next();
        T1.addEdge("E1", T2);
        T3.addEdge("E1", T4);
        T4.addEdge("E1", T5);
        g.tx().commit();
    }
    public static void createVertex8(GraphTraversalSource g) {
//        Vertex people1 = g.addV("people").property("name","user1").property("name","user2").next();
//        Vertex people2 = g.addV("people").property("name","user1").next();
//        Vertex people3 = g.addV("people").property("name","user2").next();
//        Vertex people4 = g.addV("people").property("name","user3").next();
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            Vertex people = g.addV("people").property("name", "user3").next();
            if (people.id().toString().length() >= 12) System.out.println("111111111111");
        }
//        people1.addEdge("know",people2);
//        people1.addEdge("know",people3);
//        people1.addEdge("know",people4);
        g.tx().commit();
    }

    public static void createVertex7(GraphTraversalSource g) {
        Vertex apply1 = g.V(208322936992L).next();
        Vertex apply2 = g.V(56239538360L).next();
        Vertex company = g.V(82238812240L).next();
        Vertex promoter = g.V(16902213720L).next();
        Vertex promoterApply = g.V(1581088904L).next();
        Vertex device = g.V(235966804128L).next();
        Vertex email = g.V(5490061360L).next();
        apply1.addEdge("UseCompany", company).property("amt", 4101901);
        apply1.addEdge("UsePromoter", promoter).property("amt", 4101313);
        apply1.addEdge("UsePromoterApply", promoterApply).property("amt", 4101341);
        apply2.addEdge("UseDevice", device).property("amt", 9519912);
        apply2.addEdge("UseEmail", email).property("amt", 9519912);
        apply2.addEdge("UsePromoter", promoter).property("amt", 4101314);
        apply2.addEdge("UsePromoterApply", promoterApply).property("amt", 4101342);
        g.tx().commit();
    }

    public static void createVertex4(GraphTraversalSource g) {
        Vertex user1 = g.addV("frameno").property("name", "user1").property("value", "4").next();
        Vertex user2 = g.addV("frameno").property("name", "user2").property("value", "5").next();
        Vertex user3 = g.addV("frameno").property("name", "user3").property("value", "6").next();
        Vertex merchant1 = g.addV("lazb_registno").property("name", "merchant1").property("value", "99").next();
        Vertex merchant2 = g.addV("lazb_registno").property("name", "merchant2").property("value", "98").next();
        Vertex merchant3 = g.addV("lazb_registno").property("name", "merchant3").property("value", "97").next();
        user1.addEdge("sgglc", merchant1).property("transtime", 1548489996974l);
        user2.addEdge("sgglc", merchant1).property("transtime", 1548489996974l);
        user2.addEdge("sgglc", merchant2).property("transtime", 1548489996974l);
        user3.addEdge("sgglc", merchant2).property("transtime", 1548489996974l);
        user3.addEdge("sgglc", merchant3).property("transtime", 1548489996974l);
        user1.addEdge("sgglc", merchant3).property("transtime", 1548489996974l);
        g.tx().commit();
    }

    public static void createVertex5(GraphTraversalSource g) {
        Vertex car = g.addV("frameno").property("name", "user1").property("value", "4").next();
        Vertex car2 = g.addV("frameno").property("name", "user2").property("value", "5").next();
        Vertex trace = g.addV("lazb_registno").property("name", "merchant1").property("value", "99").next();
        Vertex trace2 = g.addV("lazb_registno").property("name", "merchant2").property("value", "98").next();
        Vertex p = g.addV("bdx_insurecode").property("name", "p1").property("value", "98").next();
        Vertex p2 = g.addV("bdx_insurecode").property("name", "p2").property("value", "98").next();

        Vertex car3 = g.addV("frameno").property("name", "user1").property("value", "4").next();
        Vertex car4 = g.addV("frameno").property("name", "user2").property("value", "5").next();
        Vertex trace3 = g.addV("lazb_registno").property("name", "merchant1").property("value", "99").next();
        Vertex trace4 = g.addV("lazb_registno").property("name", "merchant2").property("value", "98").next();
        Vertex p3 = g.addV("bdx_insurecode").property("name", "p1").property("value", "98").next();
        Vertex p4 = g.addV("bdx_insurecode").property("name", "p2").property("value", "98").next();

        Vertex pay = g.addV("lpb_payeebankaccountname").property("name", "pay").property("value", "98").next();
        Vertex pay2 = g.addV("lpb_payeebankaccountname").property("name", "pay2").property("value", "98").next();

        pay.addEdge("sgskr", trace).property("transtime", 1548489996974l);
        pay.addEdge("sgskr", trace2).property("transtime", 1548489996974l);
        trace.addEdge("sgglc", car).property("transtime", 1548489996974l);
        trace2.addEdge("sgglc", car2).property("transtime", 1548489996974l);
        car.addEdge("cbbxr", p).property("transtime", 1548489996974l);
        car2.addEdge("cbbxr", p2).property("transtime", 1548489996974l);

        pay2.addEdge("sgskr", trace3).property("transtime", 1548489996974l);
        pay2.addEdge("sgskr", trace4).property("transtime", 1548489996974l);
        trace3.addEdge("sgglc", car3).property("transtime", 1548489996974l);
        trace4.addEdge("sgglc", car3).property("transtime", 1548489996974l);
        car3.addEdge("cbbxr", p3).property("transtime", 1548489996974l);
        car3.addEdge("cbbxr", p4).property("transtime", 1548489996974l);
        g.tx().commit();
    }

    public static void createVertex6(GraphTraversalSource g) {
        Vertex car = g.addV("frameno").property("name", "user1").property("value", "4").next();
        Vertex car2 = g.addV("frameno").property("name", "user2").property("value", "5").next();
        Vertex trace = g.addV("lazb_registno").property("name", "merchant1").property("value", "99").next();
        Vertex trace2 = g.addV("lazb_registno").property("name", "merchant2").property("value", "98").next();
        Vertex p = g.addV("bdx_insurecode").property("name", "p1").property("value", "98").next();
        Vertex p2 = g.addV("bdx_insurecode").property("name", "p2").property("value", "98").next();

        Vertex car3 = g.addV("frameno").property("name", "user1").property("value", "4").next();
        Vertex car4 = g.addV("frameno").property("name", "user2").property("value", "5").next();
        Vertex trace3 = g.addV("lazb_registno").property("name", "merchant1").property("value", "99").next();
        Vertex trace4 = g.addV("lazb_registno").property("name", "merchant2").property("value", "98").next();
        Vertex p3 = g.addV("bdx_insurecode").property("name", "p1").property("value", "98").next();
        Vertex p4 = g.addV("bdx_insurecode").property("name", "p2").property("value", "98").next();

        car.addEdge("cbbxr", p).property("transtime", 1548489996974l);
        car2.addEdge("cbbxr", p).property("transtime", 1548489996974l);
        car.addEdge("sgglc", trace).property("transtime", 1548489996974l);
        car2.addEdge("sgglc", trace).property("transtime", 1548489996974l);

        car3.addEdge("cbbxr", p2).property("transtime", 1548489996974l);
        car4.addEdge("cbbxr", p2).property("transtime", 1548489996974l);
        car3.addEdge("sgglc", trace3).property("transtime", 1548489996974l);
        car4.addEdge("sgglc", trace4).property("transtime", 1548489996974l);

        g.tx().commit();
    }

    public static void createVertex(GraphTraversalSource g) {
        Vertex phone1 = g.addV("phone").property("name", "iphone1").property("value", "1").next();
        Vertex phone2 = g.addV("phone").property("name", "iphone2").property("value", "2").next();
        Vertex phone3 = g.addV("phone").property("name", "iphone3").property("value", "3").next();
        Vertex user1 = g.addV("card").property("name", "user1").property("value", "4").next();
        Vertex user2 = g.addV("card").property("name", "user2").property("value", "5").next();
        Vertex user3 = g.addV("card").property("name", "user3").property("value", "6").next();
        Vertex user4 = g.addV("card").property("name", "user3").property("value", "7").next();
        Vertex user5 = g.addV("card").property("name", "user3").property("value", "8").next();
        Vertex user6 = g.addV("card").property("name", "user3").property("value", "9").next();
        Vertex user7 = g.addV("card").property("name", "user3").property("value", "10").next();
        Vertex user8 = g.addV("card").property("name", "user3").property("value", "11").next();
        Vertex merchant1 = g.addV("merchant").property("name", "merchant1").property("value", "99").next();
        Vertex merchant2 = g.addV("merchant").property("name", "merchant2").property("value", "98").next();
        Vertex merchant3 = g.addV("merchant").property("name", "merchant3").property("value", "97").next();
        Vertex merchant4 = g.addV("merchant").property("name", "merchant4").property("value", "96").next();
        Vertex merchant5 = g.addV("merchant").property("name", "merchant5").property("value", "95").next();
        Vertex merchant6 = g.addV("merchant").property("name", "merchant6").property("value", "94").next();
        Vertex merchant7 = g.addV("merchant").property("name", "merchant7").property("value", "93").next();
        Vertex merchant8 = g.addV("merchant").property("name", "merchant8").property("value", "92").next();
        user1.addEdge("Pay", merchant1).property("transtime", 1548489996974l);
        user1.addEdge("Pay", merchant2).property("transtime", 1548489996974l);
        user1.addEdge("Pay", merchant3).property("transtime", 1548489996974l);
        user2.addEdge("Pay", merchant1).property("transtime", 1548489996974l);
        user2.addEdge("Pay", merchant2).property("transtime", 1548489996974l);
        user2.addEdge("Pay", merchant3).property("transtime", 1548489996974l);
        user2.addEdge("Pay", merchant4).property("transtime", 1548489996974l);
        user3.addEdge("Pay", merchant1).property("transtime", 1548489996974l);
        user3.addEdge("Pay", merchant2).property("transtime", 1548489996974l);
        user4.addEdge("Pay", merchant1).property("transtime", 1548489996974l);
        user5.addEdge("Pay", merchant5).property("transtime", 1548489996974l);
        user5.addEdge("Pay", merchant6).property("transtime", 1548489996974l);
        user5.addEdge("Pay", merchant7).property("transtime", 1548489996974l);
        user6.addEdge("Pay", merchant5).property("transtime", 1548489996974l);
        user6.addEdge("Pay", merchant6).property("transtime", 1548489996974l);
        user6.addEdge("Pay", merchant7).property("transtime", 1548489996974l);
        user6.addEdge("Pay", merchant8).property("transtime", 1548489996974l);
        user7.addEdge("Pay", merchant5).property("transtime", 1548489996974l);
        user7.addEdge("Pay", merchant6).property("transtime", 1548489996974l);
        user8.addEdge("Pay", merchant5).property("transtime", 1548489996974l);
        user1.addEdge("UsePhone", phone2);
        user2.addEdge("UsePhone", phone3);
        user3.addEdge("UsePhone", phone1);
        g.tx().commit();
    }

    public static void createVertex3(GraphTraversalSource g) {
        Vertex user1 = g.addV("card").property("name", "user1").property("value", "4").next();
        Vertex merchant1 = g.addV("merchant").property("name", "merchant1").property("value", "99").next();
        user1.addEdge("Pay", merchant1).property("transtime", 1548489996974l);
        g.tx().commit();
    }

    public static void createVertex2(GraphTraversalSource g) {
        Vertex card0 = g.addV("card").property("name", "card0").property("value", "0").next();
        for (int i = 1; i <= 1000000; i++) {
            Vertex merchant1 = g.addV("merchant").property("name", "merchant" + i).property("value", String.valueOf(i)).next();
            Edge e = card0.addEdge("Pay", merchant1);
            e.property("transtime", 1548489996974L + i);
            e.property("name", "pay" + i);
        }
        g.tx().commit();
    }

    public static void process(GraphTraversalSource g) {
        System.out.println(g.V().hasLabel("user").peerPressure().by("cluster").by(__.bothE().bothE()).values("cluster").dedup().groupCount().next());
    }

    public static Vertex getOrCreateVertex(GraphTraversalSource g, long id) {
        Iterator<Vertex> node = g.V().has("user_id", String.valueOf(id)).hasLabel("user");
        if (node.hasNext()) {
            return node.next();
        } else {
            return g.addV("user").property("user_id", String.valueOf(id)).next();
        }
    }

    public static Edge getOrCreateEdge(GraphTraversalSource g, Vertex node_1, Vertex node_2) {
        Iterator<Edge> edge = node_1.edges(Direction.BOTH, "followed_by");
        if (edge.hasNext()) {
            Edge e = edge.next();
            if (e.outVertex().id() == node_2.id() || e.inVertex().id() == node_2.id()) {
                return e;
            } else {
                Edge add = node_1.addEdge("followed_by", node_2);
                return add;
            }
        } else {
            return node_1.addEdge("followed_by", node_2);
        }
    }
}

class CustomRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
            // 核心改造点，由blockingqueue的offer改成put阻塞方法
            executor.getQueue().put(r);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class InsertTask implements Callable<Void> {
    private int amt;
    private JanusGraph graph;

    public InsertTask(JanusGraph graph, int amt) {
        this.amt = amt;
        this.graph = graph;
    }

    @Override
    public Void call() throws Exception {
        GraphTraversalSource g = graph.traversal();
        g.V().has("id", "1").as("start").V().has("id", "2").as("end")
            .V().addE("trade").from(__.select("start").unfold()).to(__.select("end").unfold()).property("amt", 1).next();
        g.tx().commit();
        g.close();
        return null;
    }
}