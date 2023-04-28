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

package org.janusgraph.graphdb.types;

import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.VertexLabel;

/**
 * 根据从cell中取出来的label id（点label 边label 属性label） 获取定义。
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TypeInspector {

    /**
     * 获取属性的Pojo，是直接从关系中直接去取的。
     * @param id
     * @return
     */
    default PropertyKey getExistingPropertyKey(long id) {
        return (PropertyKey)getExistingRelationType(id);
    }

    default EdgeLabel getExistingEdgeLabel(long id) {
        return (EdgeLabel)getExistingRelationType(id);
    }

    /**
     * 根据schema id 获取 label实体
     * @param id
     * @return
     */
    RelationType getExistingRelationType(long id);

    VertexLabel getExistingVertexLabel(long id);

    boolean containsRelationType(String name);

    RelationType getRelationType(String name);

}
