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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * The multiplicity of edges between vertices for a given label. Multiplicity here is understood in the same sense as
 * for UML class diagrams <a href="https://en.wikipedia.org/wiki/Class_diagram#Multiplicity">https://en.wikipedia.org/wiki/Class_diagram#Multiplicity</a>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum Multiplicity {

    /**
     * The given edge label specifies a multi-graph, meaning that the multiplicity is not constrained and that
     * there may be multiple edges of this label between any given pair of vertices.
     *
     * <a href="https://en.wikipedia.org/wiki/Multigraph">https://en.wikipedia.org/wiki/Multigraph</a>
     */
    MULTI,

    /**
     * The given edge label specifies a simple graph, meaning that the multiplicity is not constrained but that there
     * can only be at most a single edge of this label between a given pair of vertices.
     */
    SIMPLE,

    /**
     * 在图中的任何顶点上最多允许此类标签的一个传入边，但对传出边没有限制。边缘标签winnerOf是一个具有 ONE2MANY 多重性的示例，因为每个比赛最多由一个人赢得，但一个人可以赢得多个比赛。
     * There can only be a single in-edge of this label for a given vertex but multiple out-edges (i.e. in-unique)
     */
    ONE2MANY,

    /**
     * There can only be a single out-edge of this label for a given vertex but multiple in-edges (i.e. out-unique)
     * 在图中的任何顶点上最多允许此类标签的一个出边，但对传入边没有限制：
     * 比如人人之间的mother标签， 人出边只能有一个，表示人只有一个母亲，但是入边可以有多个，多个人的母亲是同一个。
     */
    MANY2ONE,

    /**
     * There can be only a single in and out-edge of this label for a given vertex (i.e. unique in both directions).
     */
    ONE2ONE;


    /**
     * Whether this multiplicity imposes any constraint on the number of edges that may exist between a pair of vertices.
     *
     * @return
     */
    public boolean isConstrained() {
        return this!=MULTI;
    }

    public boolean isConstrained(Direction direction) {
        if (direction == Direction.BOTH) return isConstrained();
        return this != MULTI && (this == SIMPLE || isUnique(direction));
    }


    /**
     * If this multiplicity implies edge uniqueness in the given direction for any given vertex.
     *
     * @param direction
     * @return
     */
    public boolean isUnique(Direction direction) {
        switch (direction) {
            case IN:
                return this==ONE2MANY || this==ONE2ONE;
            case OUT:
                return this==MANY2ONE || this==ONE2ONE;
            case BOTH:
                return this==ONE2ONE;
            default: throw new AssertionError("Unknown direction: " + direction);
        }
    }

    //######### CONVERTING MULTIPLICITY <-> CARDINALITY ########

    public static Multiplicity convert(Cardinality cardinality) {
        Preconditions.checkNotNull(cardinality);
        switch(cardinality) {
            case LIST: return MULTI;
            case SET: return SIMPLE;
            case SINGLE: return MANY2ONE;
            default: throw new AssertionError("Unknown cardinality: " + cardinality);
        }
    }

    public Cardinality getCardinality() {
        switch (this) {
            case MULTI: return Cardinality.LIST;
            case SIMPLE: return Cardinality.SET;
            case MANY2ONE: return Cardinality.SINGLE;
            default: throw new AssertionError("Invalid multiplicity: " + this);
        }
    }

}
