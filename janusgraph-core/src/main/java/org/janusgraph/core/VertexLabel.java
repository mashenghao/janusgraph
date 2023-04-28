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

import org.janusgraph.core.schema.JanusGraphSchemaType;

import java.util.Collection;

/**
 * 标记是 label-> 点label
 *
 * 点Label是附属在点上的属性，可以被用作定义点的性质。包括点有哪些属性，点有哪些关系。
 * A vertex label is a label attached to vertices in a JanusGraph graph. This can be used to define the nature of a
 * vertex.
 * <p>
 * Internally, a vertex label is also used to specify certain characteristics of vertices that have a given label.
 * 被点 实现接口，代表着这个点，具有这些点label的特征。点的属性所属于点label，而不是点。
 * JanusGraphVertex 是父接口，标记实现这个接口的类，可以具备这两种特性。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabel extends JanusGraphVertex, JanusGraphSchemaType {

    /**
     * Whether vertices with this label are partitioned. 该label类型的点是否还进行分区。
     *
     * @return
     */
    boolean isPartitioned();

    /**
     * Whether vertices with this label are static, that is, immutable beyond the transaction
     * in which they were created.
     *带有此标签的顶点是否是静态的，即在创建它们的事务之外是不可变的。
     * @return
     */
    boolean isStatic();

    //TTL

    /**
     * 这个点lable下的所有属性。
     * Collects all property constraints.
     *
     * @return a list of {@link PropertyKey} which represents all property constraints for a {@link VertexLabel}.
     */
    Collection<PropertyKey> mappedProperties();

    /**
     * 点label对应的关系。
     * Collects all connection constraints.
     *
     * @return a list of {@link Connection} which represents all connection constraints for a {@link VertexLabel}.
     */
    Collection<Connection> mappedConnections();

}
