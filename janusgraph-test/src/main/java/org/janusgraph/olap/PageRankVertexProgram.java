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

package org.janusgraph.olap;

import com.google.common.collect.ImmutableSet;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.commons.configuration.Configuration;

import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

/**
 * This implementation is only intended for testing.
 * <p>
 * Limitations:
 * <ul>
 * <li>Ignores edge labels</li>
 * <li>Assumes there is at most one edge (of any label) between any vertex pair</li>
 * </ul>
 * This is almost an exact copy of the TinkerPop 3 PR implementation.
 */
public class PageRankVertexProgram extends StaticVertexProgram<Double> {


    public static final String PAGE_RANK = "janusgraph.pageRank.pageRank";
    public static final String OUTGOING_EDGE_COUNT = "janusgraph.pageRank.edgeCount";

    private static final String DAMPING_FACTOR = "janusgraph.pageRank.dampingFactor";
    private static final String MAX_ITERATIONS = "janusgraph.pageRank.maxIterations";
    private static final String VERTEX_COUNT = "janusgraph.pageRank.vertexCount";

    private double dampingFactor;
    private int maxIterations;
    private long vertexCount;

    private final MessageScope.Local<Double> outE = MessageScope.Local.of(__::outE);
    private final MessageScope.Local<Double> inE = MessageScope.Local.of(__::inE);

    private static final Set<VertexComputeKey> COMPUTE_KEYS = ImmutableSet.of(VertexComputeKey.of(PAGE_RANK, false), VertexComputeKey.of(OUTGOING_EDGE_COUNT, false));

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        dampingFactor = configuration.getDouble(DAMPING_FACTOR, 0.85D);
        maxIterations = configuration.getInt(MAX_ITERATIONS, 10);
        vertexCount = configuration.getLong(VERTEX_COUNT, 1L);
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(VERTEX_PROGRAM, PageRankVertexProgram.class.getName());
        configuration.setProperty(DAMPING_FACTOR, dampingFactor);
        configuration.setProperty(MAX_ITERATIONS, maxIterations);
        configuration.setProperty(VERTEX_COUNT, vertexCount);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return COMPUTE_KEYS;
    }

    @Override
    public void setup(Memory memory) {
    }

    @Override
    public void execute(Vertex vertex, Messenger<Double> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            //首次迭代，为该点的所有入边的始点，发送double类型的消息为 1D。
            messenger.sendMessage(inE, 1D);
        } else if (1 == memory.getIteration()) {
            //1. 第一次迭代: 聚合每个点收到的消息，这个消息的意思是，该点有几个出边。
            double initialPageRank = 1D / vertexCount;
            double edgeCount = IteratorUtils.stream(messenger.receiveMessages()).reduce(0D, (a, b) -> a + b);

            //2. 给每个点，添加两个computeKeys，一个是pagerank，一个是edgeCount出边数量。
            vertex.property(VertexProperty.Cardinality.single, PAGE_RANK, initialPageRank);
            vertex.property(VertexProperty.Cardinality.single, OUTGOING_EDGE_COUNT, edgeCount);

            //3. 给该点的所有出边的的终点，即outV，即给该点发送消息的计算点，发送值是 1/该点入边数量。
            messenger.sendMessage(outE, initialPageRank / edgeCount);

        } else {
            //1. 聚合该点每个inE的outV(与该点有入边的点) a<- otherV,发过来的otherV的边的 1/出边数。 计算点的inV， 如果inV的出边越少，
            //则计算点的pagerank值越大。
            double newPageRank = IteratorUtils.stream(messenger.receiveMessages()).reduce(0D, (a, b) -> a + b);

            //2. 算出该点的PageRank值。 固定算法，反正点的入边越多，pagerank越大。
            newPageRank =  (dampingFactor * newPageRank) + ((1D - dampingFactor) / vertexCount); //(0.85*rank) + (0.15/1)

            //3. 设置点的pagerank值，因为这个是计算属性，需要放到点上面，为了下次迭代计算的时候，将数据传入出去。
            vertex.property(VertexProperty.Cardinality.single, PAGE_RANK, newPageRank);

            //4. 给计算点的outV，发送消息， 消息值是pagerank/点的出边数。
            messenger.sendMessage(outE, newPageRank / vertex.<Double>value(OUTGOING_EDGE_COUNT));
        }
    }

    @Override
    public boolean terminate(Memory memory) {
        return memory.getIteration() >= maxIterations;
    }

    @Override
    public Set<MessageScope> getMessageScopes(Memory memory) {
        return ImmutableSet.of(outE, inE);
    }

/*    @Override
    public <P extends WriteBackService> Class<P> getServiceClass() throws ClassNotFoundException {
        return null;
    }*/

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(PageRankVertexProgram.class);
        }

        public Builder vertexCount(final long vertexCount) {
            configuration.setProperty(VERTEX_COUNT, vertexCount);
            return this;
        }

        public Builder dampingFactor(final double dampingFactor) {
            configuration.setProperty(DAMPING_FACTOR, dampingFactor);
            return this;
        }

        public Builder iterations(final int iterations) {
            configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }
    }
}
