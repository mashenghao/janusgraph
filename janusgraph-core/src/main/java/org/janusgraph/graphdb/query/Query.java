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

package org.janusgraph.graphdb.query;

/**
 * Standard Query interface specifying that a query may have a limit.
 * 指定查询可能有限制的标准查询接口。
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Query {

    int NO_LIMIT = Integer.MAX_VALUE;

    /**
     * Whether this query has a defined limit
     *
     * @return
     */
    boolean hasLimit();

    /**
     *
     * @return The maximum number of results this query should return
     */
    int getLimit();



}
