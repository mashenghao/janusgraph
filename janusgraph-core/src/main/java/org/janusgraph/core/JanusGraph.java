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

package org.janusgraph.core;

import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Gremlin;

/**
 * JanusGraph graph database implementation of the Blueprint's interface.
 * Use {@link JanusGraphFactory} to open and configure JanusGraph instances.
 *  janusgraph图实现。
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see JanusGraphFactory
 * @see JanusGraphTransaction
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn("org.janusgraph.blueprints.process.traversal.strategy.JanusGraphStrategySuite")
//------------------------
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest$VertexPropertyAddition",
        method = "shouldHandleSetVertexProperties",
        reason = "JanusGraph can only handle SET cardinality for properties when defined in the schema.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldOnlyAllowReadingVertexPropertiesInMapReduce",
        reason = "JanusGraph simply throws the wrong exception -- should not be a ReadOnly transaction exception but a specific one for MapReduce. This is too cumbersome to refactor in JanusGraph.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldProcessResultGraphNewWithPersistVertexProperties",
        reason = "The result graph should return an empty iterator when vertex.edges() or vertex.vertices() is called.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithNoEdgeLabels",
        reason = "JanusGraph does not support default edge label (edge) used when GraphML is missing edge labels.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.structure.io.IoTest$GraphMLTest",
        method = "shouldReadGraphMLWithoutEdgeIds",
        reason = "JanusGraph does not support default edge label (edge) used when GraphML is missing edge ids.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.GraphComputerTest",
        method = "shouldSupportGraphFilter",
        reason = "JanusGraph test graph computer (FulgoraGraphComputer) " +
            "currently does not support graph filters but does not throw proper exception because doing so breaks numerous " +
            "tests in gremlin-test ProcessComputerSuite.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgramTest",
        method = "*",
        reason = "ShortestPathVertexProgram currently has two bugs that prevent us from using it correctly. See " +
            "https://issues.apache.org/jira/browse/TINKERPOP-2187 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ShortestPathTest$Traversals",
        method = "*",
        reason = "ShortestPathVertexProgram currently has two bugs that prevent us from using it correctly. See " +
            "https://issues.apache.org/jira/browse/TINKERPOP-2187 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX",
        reason = "The test assumes that a certain vertex has always the lowest id which is not the case for " +
            "JanusGraph. See https://issues.apache.org/jira/browse/TINKERPOP-2189 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "g_V_connectedComponent_withXEDGES_bothEXknowsXX_withXPROPERTY_NAME_clusterX_project_byXnameX_byXclusterX",
        reason = "The test assumes that a certain vertex has always the lowest id which is not the case for " +
            "JanusGraph. See https://issues.apache.org/jira/browse/TINKERPOP-2189 for more information.")
@Graph.OptOut(
    test = "org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest",
    method = "g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values",
    reason = "The test assumes that a certain vertex has always the lowest id which is not the case for " +
        "JanusGraph. See https://issues.apache.org/jira/browse/TINKERPOP-2189 for more information.")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ConnectedComponentTest",
        method = "g_V_dedup_connectedComponent_hasXcomponentX",
        reason = "The test involves serializing and deserializing of vertices, especially of CacheVertex. This class" +
            "is however not serializable and it is non-trivial to enable serialization as the class is tied to a" +
            "transaction. See #1519 for more information.")
public interface JanusGraph extends Transaction {

   /* ---------------------------------------------------------------
    * Transactions and general admin
    * ---------------------------------------------------------------
    */

    /**
     * 打开一个线程无关的事务
     * Opens a new thread-independent {@link JanusGraphTransaction}.
     * <p>
     * The transaction is open when it is returned but MUST be explicitly（关闭） closed by calling {@link org.janusgraph.core.JanusGraphTransaction#commit()}
     * or {@link org.janusgraph.core.JanusGraphTransaction#rollback()} when it is no longer needed.
     * <p>
     * Note, that this returns a thread independent transaction object. It is not necessary to call this method
     * to use Blueprint's standard transaction framework which will automatically start a transaction with the first
     * operation on the graph.
     *
     * @return Transaction object representing a transactional context.
     */
    JanusGraphTransaction newTransaction();

    /**
     * 创建一个事务构建器，用于构建一个事务。
     * Returns a {@link TransactionBuilder} to construct a new thread-independent {@link JanusGraphTransaction}.
     *
     * @return a new TransactionBuilder
     * @see TransactionBuilder
     * @see #newTransaction()
     */
    TransactionBuilder buildTransaction();

    /**
     * 返回一个该图实例的schema管理类。 提供方法改变全局的配置，创建索引，等其他操作。
     * 这个 JanusGraphManagement 是SchemaInspector的子类，定义了所有的约束创建的实现。
     * Returns the management system for this graph instance. The management system provides functionality
     * to change global configuration options, install indexes and inspect the graph schema.
     * <p>
     * The management system operates in its own transactional context which must be explicitly（明确地） closed.
     *
     * @return
     */
    JanusGraphManagement openManagement();

    /**
     * Checks whether the graph is open.
     *
     * @return true, if the graph is open, else false.
     * @see #close()
     */
    boolean isOpen();

    /**
     * Checks whether the graph is closed.
     *
     * @return true, if the graph has been closed, else false
     */
    boolean isClosed();

    /**
     * Closes the graph database.
     * <p>
     * Closing the graph database causes a disconnect and possible closing of the underlying storage backend
     * and a release of all occupied resources by this graph database.
     * Closing a graph database requires that all open thread-independent transactions have been closed -
     * otherwise they will be left abandoned.
     *
     * @throws JanusGraphException if closing the graph database caused errors in the storage backend
     */
    @Override
    void close() throws JanusGraphException;

    /**
     * The version of this JanusGraph graph database
     *
     * @return
     */
    static String version() {
        return JanusGraphConstants.VERSION;
    }

    static void main(String[] args) {
        System.out.println("JanusGraph " + JanusGraph.version() + ", Apache TinkerPop " + Gremlin.version());
    }
}
