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

package org.janusgraph.diskstorage.locking;

import com.google.common.collect.MapMaker;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.KeyColumn;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 *记录每个事务对象，持有了那些数据的锁， 存储结构是map<事务对象,<lockID,Hbase当前进程id写入的记录>>。
 * 必须获取到本地锁后 并写入到hbase记录中成功后才写入记录。
 *
 *
 * A store for {@code LockStatus} objects. Thread-safe so long as the method
 * calls with any given {@code StoreTransaction} are serial. Put another way,
 * thread-safety is only broken by concurrently calling this class's methods
 * with the same {@code StoreTransaction} instance in the arguments to each
 * concurrent call.
 *
 * @see AbstractLocker
 * @param <S>
 *            The {@link LockStatus} type.
 */
public class LockerState<S> {

    /**
     * Locks taken in the LocalLockMediator and written to the store (but not
     * necessarily checked)
     * k：事务对象
     * v: 当前事务持有的锁集合：
     *  k：锁编号
     *  v:锁的状态，详细信息。
     *
     *  记录当前事务，持有了那些数据的锁。
     */
    private final ConcurrentMap<StoreTransaction, Map<KeyColumn, S>> locks;

    public LockerState() {
        // TODO this wild guess at the concurrency level should not be hardcoded
        this(new MapMaker().concurrencyLevel(8).weakKeys()
                .makeMap());
    }

    public LockerState(ConcurrentMap<StoreTransaction, Map<KeyColumn, S>> locks) {
        this.locks = locks;
    }

    public boolean has(StoreTransaction tx, KeyColumn kc) {
        return getLocksForTx(tx).containsKey(kc);
    }

    public void take(StoreTransaction tx, KeyColumn kc, S ls) {
        getLocksForTx(tx).put(kc, ls);
    }

    public void release(StoreTransaction tx, KeyColumn kc) {
        getLocksForTx(tx).remove(kc);
    }

    public Map<KeyColumn, S> getLocksForTx(StoreTransaction tx) {
        Map<KeyColumn, S> m = locks.get(tx);

        if (null == m) {
            m = new HashMap<>();
            final Map<KeyColumn, S> x = locks.putIfAbsent(tx, m);
            if (null != x) {
                m = x;
            }
        }

        return m;
    }
}
