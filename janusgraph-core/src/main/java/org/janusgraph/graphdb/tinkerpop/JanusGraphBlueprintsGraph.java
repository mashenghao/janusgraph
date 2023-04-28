// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Blueprints specific implementation for {@link JanusGraph}.
 * Handles thread-bound transactions.
 * 处理线程绑定的事务用的。 将事务绑定到线程上。这个类就是 将所有涉及事务的操作，绑定到线程上。
 * getAutoStartTx()方法，是重点。 他返回的是一个JanusGraphTranction对象，同时实现了Graph的接口，负责分担StandJanusGRaph的事务查询，
 * 以后的查询，走vertexs() 或者edges()等查询实际是走的这个实例， 通过这个事务实例做操作。 事务提交最终也是Standgraph从threadLocal取出来这个
 * 实例后，走的这个事务实例的方法。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class JanusGraphBlueprintsGraph implements JanusGraph {

    private static final Logger log =
            LoggerFactory.getLogger(JanusGraphBlueprintsGraph.class);




    // ########## TRANSACTION HANDLING ###########################
    //创建了一个tinkerpopTxContainer的事务处理对象，
    final GraphTransaction tinkerpopTxContainer = new GraphTransaction();

    private ThreadLocal<JanusGraphBlueprintsTransaction> txs = ThreadLocal.withInitial(() -> null); //这是每个线程中的事务

    //用来创建一个事务，模版方法。
    public abstract JanusGraphTransaction newThreadBoundTransaction();

    //获取当前事务对象，则个方法的调用都是在 startNewTx()方法之后，如果为null，只能说明图库被close。
    private JanusGraphBlueprintsTransaction getAutoStartTx() {
        if (txs == null) throw new IllegalStateException("Graph has been closed");
        //这个方法会调用doOpen方法，
        // org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction.readWriteConsumerInternal 这个ThreadLocal
        // ，会自动调用doOpen方法。 然后将线程事务实例放到threadLocal中，如果已经有了，则不做操作。
        tinkerpopTxContainer.readWrite(); // 重要。

        JanusGraphBlueprintsTransaction tx = txs.get();
        Preconditions.checkNotNull(tx,"Invalid read-write behavior configured: " +
                "Should either open transaction or throw exception.");
        return tx;
    }

    //创建一个新的事务对象，并放入到ThreadLocal中。 创建新的事务。
    private void startNewTx() {
        JanusGraphBlueprintsTransaction tx = txs.get();
        if (tx!=null && tx.isOpen()) throw Transaction.Exceptions.transactionAlreadyOpen();
        tx = (JanusGraphBlueprintsTransaction) newThreadBoundTransaction();
        txs.set(tx);
        log.debug("Created new thread-bound transaction {}", tx);
    }

    public JanusGraphTransaction getCurrentThreadTx() {
        return getAutoStartTx();
    }


    @Override
    public synchronized void close() {
        txs = null;
    }

    //tx()无论调用多少次，都是返回的一个事务操作方法集合。
    @Override
    public Transaction tx() {
        return tinkerpopTxContainer;
    }

    @Override
    public String toString() {
        GraphDatabaseConfiguration config = ((StandardJanusGraph) this).getConfiguration();
        return StringFactory.graphString(this,config.getBackendDescription());
    }

    @Override
    public Variables variables() {
        return new JanusGraphVariables(((StandardJanusGraph)this).getBackend().getUserConfiguration());
    }

    @Override
    public Configuration configuration() {
        GraphDatabaseConfiguration config = ((StandardJanusGraph) this).getConfiguration();
        return config.getConfigurationAtOpen();
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        if (builder.requiresVersion(GryoVersion.V1_0) || builder.requiresVersion(GraphSONVersion.V1_0)) {
            return (I) builder.graph(this).onMapper(mapper ->  mapper.addRegistry(JanusGraphIoRegistryV1d0.getInstance())).create();
        } else if (builder.requiresVersion(GraphSONVersion.V2_0)) {
            return (I) builder.graph(this).onMapper(mapper ->  mapper.addRegistry(JanusGraphIoRegistry.getInstance())).create();
        } else {
            return (I) builder.graph(this).onMapper(mapper ->  mapper.addRegistry(JanusGraphIoRegistry.getInstance())).create();
        }
    }

    // ########## TRANSACTIONAL FORWARDING ###########################

    @Override
    public JanusGraphVertex addVertex(Object... keyValues) {
        return getAutoStartTx().addVertex(keyValues);
    }
    
    //根据点id 获取点，JanusGraphBlueprintsGraph为所有的操作,都添加了事务。getAutoStartTx()返回了一个janusGraphTraction对象，这个对象是Graph的子类。
    @Override//
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return getAutoStartTx().vertices(vertexIds);//JanusGraphBlueprintsTransaction 调用vertices()方法。
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return getAutoStartTx().edges(edgeIds);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        if (!graphComputerClass.equals(FulgoraGraphComputer.class)) {
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass);
        } else {
            return (C)compute();
        }
    }

    @Override
    public FulgoraGraphComputer compute() throws IllegalArgumentException {
        StandardJanusGraph graph = (StandardJanusGraph)this;
        return new FulgoraGraphComputer(graph,graph.getConfiguration().getConfiguration());
    }

    @Override
    public JanusGraphVertex addVertex(String vertexLabel) {
        return getAutoStartTx().addVertex(vertexLabel);
    }

    @Override
    public JanusGraphQuery<? extends JanusGraphQuery> query() {
        return getAutoStartTx().query();
    }

    @Override
    public JanusGraphIndexQuery indexQuery(String indexName, String query) {
        return getAutoStartTx().indexQuery(indexName,query);
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(JanusGraphVertex... vertices) {
        return getAutoStartTx().multiQuery(vertices);
    }

    @Override
    public JanusGraphMultiVertexQuery multiQuery(Collection<JanusGraphVertex> vertices) {
        return getAutoStartTx().multiQuery(vertices);
    }


    //Schema

    @Override
    public PropertyKeyMaker makePropertyKey(String name) {
        return getAutoStartTx().makePropertyKey(name);
    }

    @Override
    public EdgeLabelMaker makeEdgeLabel(String name) {
        return getAutoStartTx().makeEdgeLabel(name);
    }

    @Override
    public VertexLabelMaker makeVertexLabel(String name) {
        return getAutoStartTx().makeVertexLabel(name);
    }

    @Override
    public VertexLabel addProperties(VertexLabel vertexLabel, PropertyKey... keys) {
        return getAutoStartTx().addProperties(vertexLabel, keys);
    }

    @Override
    public EdgeLabel addProperties(EdgeLabel edgeLabel, PropertyKey... keys) {
        return getAutoStartTx().addProperties(edgeLabel, keys);
    }

    @Override
    public EdgeLabel addConnection(EdgeLabel edgeLabel, VertexLabel outVLabel, VertexLabel inVLabel) {
        return getAutoStartTx().addConnection(edgeLabel, outVLabel, inVLabel);
    }

    @Override
    public boolean containsPropertyKey(String name) {
        return getAutoStartTx().containsPropertyKey(name);
    }

    @Override
    public PropertyKey getOrCreatePropertyKey(String name) {
        return getAutoStartTx().getOrCreatePropertyKey(name);
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        return getAutoStartTx().getPropertyKey(name);
    }

    @Override
    public boolean containsEdgeLabel(String name) {
        return getAutoStartTx().containsEdgeLabel(name);
    }

    @Override
    public EdgeLabel getOrCreateEdgeLabel(String name) {
        return getAutoStartTx().getOrCreateEdgeLabel(name);
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        return getAutoStartTx().getEdgeLabel(name);
    }

    @Override
    public boolean containsRelationType(String name) {
        return getAutoStartTx().containsRelationType(name);
    }

    @Override
    public RelationType getRelationType(String name) {
        return getAutoStartTx().getRelationType(name);
    }

    @Override
    public boolean containsVertexLabel(String name) {
        return getAutoStartTx().containsVertexLabel(name);
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        return getAutoStartTx().getVertexLabel(name);
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String name) {
        return getAutoStartTx().getOrCreateVertexLabel(name);
    }


    /**
     * JanusGraph的事务实现类，是tinkerpop提供的模版API，实现doXXX()方法。
     * 在遍历时，Traversal会自动进行事务的操作，开始traversal之前打开事务，出现异常进行回滚操作。
     *
     * 这个类继承的是AbstractThreadLocalTransaction，是将所有的
     */
    class GraphTransaction extends AbstractThreadLocalTransaction {

        public GraphTransaction() {
            super(JanusGraphBlueprintsGraph.this);
        }

        @Override
        public void doOpen() {
            //这个doOpen方法也会被readWriter方法调用，有个AbstractThreadLocalTransaction的readWriteConsumerInternal的AUTO就是打开事务。
            startNewTx();
        }

        @Override
        public void doCommit() {
            getAutoStartTx().commit();
        }

        @Override
        public void doRollback() {
            getAutoStartTx().rollback();
        }

        @Override
        public JanusGraphTransaction createThreadedTx() {
            return newTransaction();
        }

        @Override
        public boolean isOpen() {
            if (null == txs) {
                // Graph has been closed
                return false;
            }
            JanusGraphBlueprintsTransaction tx = txs.get();
            return tx!=null && tx.isOpen();
        }

        @Override
        public void close() {
            close(this);
        }

        void close(Transaction tx) {
            closeConsumerInternal.get().accept(tx);
            Preconditions.checkState(!tx.isOpen(),"Invalid close behavior configured: Should close transaction. [%s]", closeConsumerInternal);
        }

        @Override
        public Transaction onReadWrite(Consumer<Transaction> transactionConsumer) {
            Preconditions.checkArgument(transactionConsumer instanceof READ_WRITE_BEHAVIOR,
                    "Only READ_WRITE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            return super.onReadWrite(transactionConsumer);
        }

        @Override
        public Transaction onClose(Consumer<Transaction> transactionConsumer) {
            Preconditions.checkArgument(transactionConsumer instanceof CLOSE_BEHAVIOR,
                    "Only CLOSE_BEHAVIOR instances are accepted argument, got: %s", transactionConsumer);
            return super.onClose(transactionConsumer);
        }
    }

}
