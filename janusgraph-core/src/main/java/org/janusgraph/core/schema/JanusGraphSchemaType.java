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

/**
 * 一共有三种，EdgeLabel，PropertyKey，VertexLabel， 这个用来定义图中实例label的Pojo， label里面有各个子类的一些方法。
 *  会将用户定义的schema结构都转为这个对象，为了操作的时候方便，比如存储的时候，可以设置标记存储到hbase中，一个label对应一个int标识，节省存储。
 *
 * 标记着是janusgraph的属性约束的接口，标记着是一类的元素schema的类型。
 * 子类有PropertyKey，VertexLabel，EdgeLabel。 这个作用的是JansGraphElement，就像当于Vertexlabel作用于
 * JanusGraphVertex。
 *
 * A JanusGraphSchemaType is a {@link JanusGraphSchemaElement} that represents a label or key
 * used in the graph. As such, a schema type is either a {@link org.janusgraph.core.RelationType}
 * or a {@link org.janusgraph.core.VertexLabel}.
 * <p>
 * JanusGraphSchemaTypes are a special {@link JanusGraphSchemaElement} in that they are referenced from the
 * main graph when creating vertices, edges, and properties.
 * 创建点边或者属性时，用来定义的约束。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphSchemaType extends JanusGraphSchemaElement {

    /**
     * Checks whether this schema type has been newly created in the current transaction.
     *标记当前 约束类型是否已经在当前事务中创建了。
     * @return True, if the schema type has been newly created, else false.
     */
    boolean isNew();
}
