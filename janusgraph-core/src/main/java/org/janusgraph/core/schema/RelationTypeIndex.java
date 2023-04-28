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

package org.janusgraph.core.schema;

import org.janusgraph.core.RelationType;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * **这是以顶点为中心的建立的局部索引**
 * 在大型图中，顶点可以有数千条关联边。遍历这些顶点可能非常慢，因为必须检索大量的入射边子集，
 * 然后在内存中进行过滤，以匹配遍历的条件。以顶点为中心的索引可以通过使用本地化索引结构来只检索需要遍历的那些边来加快这种遍历。
 *
 * 边的索引，是点中心索引，是建立一个点的所有边建立索引，加速这个点上的边查找。 可以通过边标签 边属性方式建立索引，也可以构建propertyIndex索引。
 * A RelationTypeIndex is an index installed on a {@link RelationType} to speed up vertex-centric indexes for that type.
 * A RelationTypeIndex is created via
 * {@link JanusGraphManagement#buildEdgeIndex(org.janusgraph.core.EdgeLabel, String, org.apache.tinkerpop.gremlin.structure.Direction, org.apache.tinkerpop.gremlin.process.traversal.Order, org.janusgraph.core.PropertyKey...)}
 * for edge labels and
 * {@link JanusGraphManagement#buildPropertyIndex(org.janusgraph.core.PropertyKey, String, org.apache.tinkerpop.gremlin.process.traversal.Order, org.janusgraph.core.PropertyKey...)}
 * for property keys.
 * <p>
 * This interface allows the inspection of already defined RelationTypeIndex'es. An existing index on a RelationType
 * can be retrieved via {@link JanusGraphManagement#getRelationIndex(org.janusgraph.core.RelationType, String)}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface RelationTypeIndex extends Index {

    /**
     * Returns the {@link RelationType} on which this index is installed.
     *
     * @return
     */
    RelationType getType();

    /**
     * Returns the sort order of this index. Index entries are sorted in this order and queries
     * which use this sort order will be faster.
     *
     * @return
     */
    Order getSortOrder();

    /**
     * Returns the (composite) sort key for this index. The composite sort key is an ordered list of {@link RelationType}s
     *
     * @return
     */
    RelationType[] getSortKey();

    /**
     * Returns the direction on which this index is installed. An index may cover only one or both directions.
     *
     * @return
     */
    Direction getDirection();

    /**
     * Returns the status of this index
     *
     * @return
     */
    SchemaStatus getIndexStatus();


}
