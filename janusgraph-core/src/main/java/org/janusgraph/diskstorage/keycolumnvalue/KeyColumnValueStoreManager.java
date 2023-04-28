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

package org.janusgraph.diskstorage.keycolumnvalue;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;

import java.util.Map;

/**
 * TODO: 没看懂
 * KeyColumnValueStoreManager provides the persistence context to the graph database storage backend.
 * 这个接口为图实例提供了持久化的存储上下文 ？？？
 * <p>
 * A KeyColumnValueStoreManager provides transaction handles across multiple data stores that
 * are managed by this KeyColumnValueStoreManager.
 * 这个 玩意，可能用于hbase这种多节点存储的后端系统，一个事务可能涉及多个节点，这个类提供管理的。
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeyColumnValueStoreManager extends StoreManager {

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     * 打开一个有序的数据库，通过给定的名字。 如果不存在的话，将会被创建，
     * @param name Name of database 通常是列簇的名字。
     * @return Database Handle
     * @throws org.janusgraph.diskstorage.BackendException
     *
     */
    default KeyColumnValueStore openDatabase(String name) throws BackendException {
        return openDatabase(name, StoreMetaData.EMPTY);
    }

    /**
     * 用来打开一个KeyColumnValueStore， 不存在创建，已经被发开了，直接返回。 就是创建一个数据库，对用到hbase上就是新建后一个列簇。
     * KeyColumnValueStore：可以理解为hbase的某一个列簇上的数据。KeyColumnValueStore就是操作这个列簇下的数据。
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     *
     * @param name Name of database
     * @param metaData options specific to this store
     * @return Database Handle
     * @throws org.janusgraph.diskstorage.BackendException
     *
     */
    KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException;

    /**
     * Executes multiple mutations at once. For each store (identified by a string name) there is a map of (key,mutation) pairs
     * that specifies all the mutations to execute against the particular store for that key.
     *
     * This is an optional operation. Check {@link #getFeatures()} if it is supported by a particular implementation.
     *
     * @param mutations
     * @param txh
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException;

}
