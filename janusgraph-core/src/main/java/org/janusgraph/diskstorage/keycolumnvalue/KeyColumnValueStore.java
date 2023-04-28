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

import com.google.common.collect.ImmutableList;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.List;
import java.util.Map;

/**
 * 用于访问存储层的接口，存储必须像BigTable这种。也就是每一组数据都有个rowkey。
 * 每一行的组成有键值对构成。 通过给与key，可以快速检索出子集的集合。
 * Interface to a data store that has a BigTable like representation of its data. In other words, the data store is comprised of a set of rows
 * each of which is uniquely identified by a key. Each row is composed of a column-value pairs. For a given key, a subset of the column-value
 * pairs that fall within a column interval can be quickly retrieved.
 * <p>
 * This interface provides methods for retrieving and mutating the data.  提供了检索和修改数据的方法。
 * <p>
 * In this generic representation keys, columns and values are represented as ByteBuffers. //在这种通用表示中，键、列和值被表示为ByteBuffers
 * <p>
 * See <a href="https://en.wikipedia.org/wiki/BigTable">https://en.wikipedia.org/wiki/BigTable</a>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeyColumnValueStore {

    List<Entry> NO_ADDITIONS = ImmutableList.of();
    List<StaticBuffer> NO_DELETIONS = ImmutableList.of();

    /**
     * Retrieves the list of entries (i.e. column-value pairs) for a specified query.
     * 检索这个列表 通过查询条件， （想了下，应该是从vid出发，检索边这种情况，vid作为rowkey，边作为这一行记录的column，这可以通过这种情况进行检索出来数据呢。）
     *
     * @param query Query to get results for
     * @param txh   Transaction
     * @return List of entries up to a maximum of "limit" entries
     * @throws org.janusgraph.diskstorage.BackendException when columnEnd &lt; columnStart
     * @see KeySliceQuery
     */
    EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException;

    /**
     * Retrieves the list of entries (i.e. column-value pairs) as specified by the given {@link SliceQuery} for all
     * of the given keys together.
     * 给的keys，都批量检索出来。返回一个map，key是 key，值是检索出来的list。
     * （想了下，应该是从vid出发，检索边这种情况，vid作为rowkey，边作为这一行记录的column，这可以通过这种情况进行检索出来数据呢？？）
     *
     * @param keys  List of keys
     * @param query Slicequery specifying matching entries
     * @param txh   Transaction
     * @return The result of the query for each of the given keys as a map from the key to the list of result entries.
     * @throws org.janusgraph.diskstorage.BackendException
     */
    Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException;

    /**
     * /////////////////////
     * 针对key，写入那些记录，或者删除那些记录。
     *
     * /////////////////////
     * 1.先验证是否获取到用来操作的记录的锁
     * 2.之后，写入或者更新additions或者 deletions。 这些操作都是针对key对应的那条记录。
     * <p>
     * Verifies acquisition of locks {@code txh} from previous calls to
     * {@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * , then writes supplied {@code additions} and/or {@code deletions} to
     * {@code key} in the underlying data store. Deletions are applied strictly
     * before additions. In other words, if both an addition and deletion are
     * supplied for the same column, then the column will first be deleted and
     * then the supplied Entry for the column will be added.
     * <p>
     * <p>
     * <p>
     * 不支持锁，将跳过验证。
     * Implementations which don't support locking should skip the initial lock
     * verification step but otherwise behave as described above.
     *
     * @param key       the key under which the columns in {@code additions} and
     *                  {@code deletions} will be written
     * @param additions the list of Entry instances representing column-value pairs to
     *                  create under {@code key}, or null to add no column-value pairs
     * @param deletions the list of columns to delete from {@code key}, or null to
     *                  delete no columns
     * @param txh       the transaction to use
     * @throws org.janusgraph.diskstorage.locking.PermanentLockingException if locking is supported by the implementation and at least
     *                                                                      one lock acquisition attempted by
     *                                                                      {@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     *                                                                      has failed
     */
    void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException;

    /**
     * 给指定的key和 column添加锁。 实现就是往L的那个列簇里面写入rowkey=key+column，value=进程id的一个记录，
     * 判断加锁有没有成功，就是rowkey下第一个记录是否是当前进程。
     *
     * Attempts to claim a lock on the value at the specified {@code key} and
     * {@code column} pair. These locks are discretionary.
     * <p>
     * <p>
     * <p>
     * If locking fails, implementations of this method may, but are not
     * required to, throw {@link org.janusgraph.diskstorage.locking.PermanentLockingException}.
     * This method is not required
     * to determine whether locking actually succeeded and may return without
     * throwing an exception even when the lock can't be acquired. Lock
     * acquisition is only only guaranteed to be verified by the first call to
     * {@link #mutate(StaticBuffer, List, List, StoreTransaction)} on any given
     * {@code txh}.
     * <p>
     * <p>
     * <p>
     * The {@code expectedValue} must match the actual value present at the
     * {@code key} and {@code column} pair. If the true value does not match the
     * {@code expectedValue}, the lock attempt fails and
     * {@code LockingException} is thrown. This method may check
     * {@code expectedValue}. The {@code mutate()} mutate is required to check
     * it.
     * <p>expectedValue必须匹配上key和column部分。 如果没有，则会抛出异常，所失败。
     * <p>
     * <p>
     * When this method is called multiple times on the same {@code key},
     * {@code column}, and {@code txh}, calls after the first have no effect.
     * <p>
     * <p>
     * <p>
     * Locks acquired by this method must be automatically released on
     * transaction {@code commit()} or {@code rollback()}.
     * <p>
     * <p>
     * <p>
     * Implementations which don't support locking should throw
     * {@link UnsupportedOperationException}.
     *
     * @param key           the key on which to lock
     * @param column        the column on which to lock
     * @param expectedValue the expected value for the specified key-column pair on which
     *                      to lock (null means the pair must have no value)
     * @param txh           the transaction to use
     * @throws org.janusgraph.diskstorage.locking.PermanentLockingException the lock could not be acquired due to contention with other
     *                                                                      transactions or a locking-specific storage problem
     */
    void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException;

    /**
     * Returns a {@link KeyIterator} over all keys that fall within the key-range specified by the given query and have one or more columns matching the column-range.
     * Calling {@link KeyIterator#getEntries()} returns the list of all entries that match the column-range specified by the given query.
     * <p>
     * This method is only supported by stores which keep keys in byte-order.
     * 根据范围查询，返回所有这个范围内keys列表。查询参数是两个字节数组。
     * @param query
     * @param txh
     * @return
     * @throws org.janusgraph.diskstorage.BackendException
     */
    KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException;

    /**
     * Returns a {@link KeyIterator} over all keys in the store that have one or more columns matching the column-range. Calling {@link KeyIterator#getEntries()}
     * returns the list of all entries that match the column-range specified by the given query.
     * <p>
     * This method is only supported by stores which do not keep keys in byte-order.
     *
     * @param query
     * @param txh
     * @return
     * @throws org.janusgraph.diskstorage.BackendException
     */
    KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException;
    // like current getKeys if column-slice is such that it queries for vertex state property

    /**
     * Returns the name of this store. Each store has a unique name which is used to open it.
     * 就是列簇的长名字。
     * @return store name
     * @see KeyColumnValueStoreManager#openDatabase(String)
     */
    String getName();

    /**
     * Closes this store
     *
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void close() throws BackendException;


}
