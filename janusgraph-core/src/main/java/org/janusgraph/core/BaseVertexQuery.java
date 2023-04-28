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

import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 *  1. janusgrah 基于中心点构造查询条件，创建这个JanusGraphVertexQuery的实例的必须是有点id，
 *  * 基于这个点id，设置查询边的类型，指向等， 或者设置查询的属性key。 这个查询条件是暴露给图语义层调用的，
 *  * 最终的话，落到DB层要从{@link KeySliceQuery}这种，转换为rowKey和column Filter的查询条件。
 *  *2. 针对于单点进行查询。
 *  *
 *
 * 基于顶点，构造查询条件。
 * BaseVertexQuery constructs and executes a query over incident edges or properties from the perspective of a vertex.
 * <p>
 * A VertexQuery has some JanusGraph specific convenience methods for querying for incident edges or properties.
 * Using VertexQuery proceeds in two steps:
 * 1) Define the query by specifying what to retrieve and
 * 2) execute the query for the elements to retrieve.
 * <p>
 * This is the base interface for the specific implementations of a VertexQuery. Calling {@link org.janusgraph.core.JanusGraphVertex#query()}
 * returns a {@link JanusGraphVertexQuery} for querying a single vertex.
 * Calling {@link JanusGraphTransaction#multiQuery(java.util.Collection)} returns a {@link JanusGraphMultiVertexQuery} to execute
 * the same query against multiple vertices at the same time which is typically faster.
 *
 * @see JanusGraphVertexQuery
 * @see JanusGraphMultiVertexQuery
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public interface BaseVertexQuery<Q extends BaseVertexQuery<Q>> {

    /* ---------------------------------------------------------------
    * Query Specification
    * ---------------------------------------------------------------
    */

    /**
     * 限制查询指定点的边
     * Restricts this query to only those edges that point to the given vertex.
     *
     * @param vertex
     * @return this query builder
     */
    Q adjacent(Vertex vertex);

    /**
     * 限制查询的类型，含边 与属性类型
     *
     * Query for only those relations matching one of the given relation types.
     * By default, a query includes all relations in the result set.
     *
     * @param type relation types to query for
     * @return this query
     */
    Q types(String... type);

    /**
     * Query for only those relations matching one of the given relation types.
     * By default, a query includes all relations in the result set.
     *
     * @param type relation types to query for
     * @return this query
     */
    Q types(RelationType... type);

    /**
     * 查询边的类型
     * Query for only those edges matching one of the given edge labels.
     * By default, an edge query includes all edges in the result set.
     *
     * @param labels edge labels to query for
     * @return this query
     */
    Q labels(String... labels);

    /**
     * 查询的属性集合。
     * Query for only those properties having one of the given property keys.
     * By default, a query includes all properties in the result set.
     *
     * @param keys property keys to query for
     * @return this query
     */
    Q keys(String... keys);

    /**
     * 查询的关系方向
     * Query only for relations in the given direction.
     * By default, both directions are queried.
     *
     * @param d Direction to query for
     * @return this query
     */
    Q direction(Direction d);

    /**
     * 1. 类型是属性key的话，属性或者边的有这个属性值。
     * 2. 边标签，的值。
     * Query only for edges or properties that have an incident property or unidirected edge matching the given value.
     * <p>
     * If type is a property key, then the query is restricted to edges or properties having an incident property matching
     * this key-value pair.
     *
     * If type is an edge label, then it is expected that this label is unidirected ({@link EdgeLabel#isUnidirected()}
     * and the query is restricted to edges or properties having an incident unidirectional edge pointing to the value which is
     * expected to be a {@link org.janusgraph.core.JanusGraphVertex}.
     *
     * @param type  JanusGraphType name
     * @param value Value for the property of the given key to match, or vertex to point unidirectional edge to
     * @return this query
     */
    Q has(String type, Object value);

    /**
     * Query for edges or properties that have defined property with the given key
     *
     * @param key
     * @return this query
     */
    Q has(String key);

    /**
     * Query for edges or properties that DO NOT have a defined property with the given key
     *
     * @param key
     * @return this query
     */
    Q hasNot(String key);

    /**
     * Identical to {@link #has(String, Object)} but negates the condition, i.e. matches those edges or properties
     * that DO NOT satisfy this property condition.
     *
     * @param key
     * @param value
     * @return
     */
    Q hasNot(String key, Object value);

    /**
     * 属性定有的值
     * @param key
     * @param predicate
     * @param value
     * @return
     */
    Q has(String key, JanusGraphPredicate predicate, Object value);

    /**
     * Query for those edges or properties that have a property for the given key
     * whose values lies in the interval by [start,end).
     *
     * @param key   property key
     * @param start value defining the start of the interval (inclusive)
     * @param end   value defining the end of the interval (exclusive)
     * @return this query
     */
    <T extends Comparable<?>> Q interval(String key, T start, T end);

    /**
     * Sets the retrieval limit for this query.
     * <p>
     * When setting a limit, executing this query will only retrieve the specified number of relations. Note, that this
     * also applies to counts.
     *
     * @param limit maximum number of relations to retrieve for this query
     * @return this query
     */
    Q limit(int limit);


    /**
     * 排序返回的结果，只用于点属性或者边，不能用于邻接点。 因为这里用排序，是为了排序下推，
     * 如果hbase，只需要倒着scan数据就是倒叙了。
     * Orders the relation results of this query according
     * to their property for the given key in the given order (increasing/decreasing).
     * <p>
     * Note, that the ordering always applies to the incident relations (edges/properties) and NOT
     * to the adjacent vertices even if only vertices are being returned.
     *
     * @param key   The key of the properties on which to order
     * @param order the ordering direction
     * @return
     */
    Q orderBy(String key, Order order);


}
