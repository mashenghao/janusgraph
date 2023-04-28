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

import com.google.common.collect.Iterables;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.vertices.StandardVertex;

/**
 * 1. janusgrah 基于中心点构造查询条件，创建这个JanusGraphVertexQuery的实例的必须是有点id，
 * 基于这个点id，设置查询边的类型，指向等， 或者设置查询的属性key。 这个查询条件是暴露给图语义层调用的，
 * 最终的话，落到DB层要从{@link KeySliceQuery}这种，转换为rowkey和column Filter的查询条件。
 *2. 针对于单点进行查询,返回的实例是通过{@link StandardVertex#query()} 返回的，是点实例返回的查询，
 * 因为这个查询是点中心查询。
 *
 *
 * 为单个顶点执行查询。
 * A JanusGraphVertexQuery is a VertexQuery executed for a single vertex.
 * <p>
 * 被点的query方法调用。
 * Calling {@link org.janusgraph.core.JanusGraphVertex#query()} builds such a query against the vertex
 * this method is called on. This query builder provides the methods to specify which incident edges or
 * properties to query for.
 *
 *
 * @see BaseVertexQuery
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface JanusGraphVertexQuery<Q extends JanusGraphVertexQuery<Q>> extends BaseVertexQuery<Q> {

   /* ---------------------------------------------------------------
    * Query Specification (overwrite to merge BaseVertexQuery with Blueprint's VertexQuery)
    * ---------------------------------------------------------------
    */

    @Override
    Q adjacent(Vertex vertex);

    @Override
    Q types(String... type);

    @Override
    Q types(RelationType... type);

    @Override
    Q labels(String... labels);

    @Override
    Q keys(String... keys);

    @Override
    Q direction(Direction d);

    @Override
    Q has(String type, Object value);

    @Override
    Q has(String key);

    @Override
    Q hasNot(String key);

    @Override
    Q hasNot(String key, Object value);

    @Override
    Q has(String key, JanusGraphPredicate predicate, Object value);

    @Override
    <T extends Comparable<?>> Q interval(String key, T start, T end);

    @Override
    Q limit(int limit);

    @Override
    Q orderBy(String key, Order order);


    /* ---------------------------------------------------------------
    * Query execution  查询执行，
    * ---------------------------------------------------------------
    */

    /**
     * 查询返回匹配到的边。VertexCentricQueryBuilder 实例是。
     *
     * Returns an iterable over all incident edges that match this query
     *
     * @return Iterable over all incident edges that match this query
     */
    Iterable<JanusGraphEdge> edges();


    Iterable<JanusGraphVertex> vertices();

    /**
     * Returns an iterable over all incident properties that match this query
     *
     * @return Iterable over all incident properties that match this query
     */
    Iterable<JanusGraphVertexProperty> properties();

    /**
     * Returns an iterable over all incident relations that match this query
     *
     * @return Iterable over all incident relations that match this query
     */
    Iterable<JanusGraphRelation> relations();

    /**
     * Returns the number of relations that match this query
     *
     * @return Number of relations that match this query
     */
    default long count() {
        return Iterables.size(relations());
    }

    /**
     * Returns the number of edges that match this query
     *
     * @return Number of edges that match this query
     */
    default long edgeCount() {
        return vertexIds().size();
    }

    /**
     * Returns the number of properties that match this query
     *
     * @return Number of properties that match this query
     */
    default long propertyCount() {
        return Iterables.size(properties());
    }

    /**
     * Retrieves all vertices connected to this query's base vertex by edges
     * matching the conditions defined in this query.
     * <p>
     * The query engine will determine the most efficient way to retrieve the vertices that match this query.
     *
     * @return A list of all vertices connected to this query's base vertex by matching edges
     */
    VertexList vertexIds();


}
