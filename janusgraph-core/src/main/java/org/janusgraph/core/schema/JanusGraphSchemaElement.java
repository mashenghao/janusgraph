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

import org.janusgraph.core.Namifiable;

/**
 *
 * 标记是janusgraph图schema的约束的一部分。
 * Marks any element that is part of a JanusGraph Schema.
 * JanusGraph Schema elements can be uniquely identified by their name.
 * 元素可以被他们的唯一名字获取到实例。
 * <p>
 * A JanusGraph Schema element is either a {@link JanusGraphSchemaType} or an index definition, i.e.
 * {@link JanusGraphIndex} or {@link RelationTypeIndex}.
 *
 *一个是janusgraph schema的类型，包括子类，RelationType（PropertyKey/EdgeLabel）  / VertexLabel
 * 一个是索引的约束类型。
 * {@link }创建索引的类。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface JanusGraphSchemaElement extends Namifiable {

}
