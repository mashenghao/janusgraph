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

package org.janusgraph.graphdb.database.idassigner;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDBlock;

import org.janusgraph.diskstorage.IDAuthority;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardIDPool implements IDPool {

    private static final Logger log =
            LoggerFactory.getLogger(StandardIDPool.class);


    private static final IDBlock ID_POOL_EXHAUSTION = new IDBlock() {
        @Override
        public long numIds() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getId(long index) {
            throw new UnsupportedOperationException();
        }
    };

    private static final IDBlock UNINITIALIZED_BLOCK = new IDBlock() {
        @Override
        public long numIds() {
            return 0;
        }

        @Override
        public long getId(long index) {
            throw new ArrayIndexOutOfBoundsException(0);
        }
    };

    private static final int RENEW_ID_COUNT = 100;

    private final IDAuthority idAuthority;
    private final long idUpperBound; //exclusive
    private final int partition;
    private final int idNamespace;

    private final Duration renewTimeout;
    private final double renewBufferPercentage;

    private IDBlock currentBlock;
    private long currentIndex;
    private long renewBlockIndex;
//    private long nextID;
//    private long currentMaxID;
//    private long renewBufferID;

    private volatile IDBlock nextBlock;
    private Future<IDBlock> idBlockFuture;
    private IDBlockGetter idBlockGetter;
    private final ThreadPoolExecutor exec;

    private volatile boolean closed;

    private final Queue<Future<?>> closeBlockers;

    /**
     * 创建一个idpool
     *
     * @param idAuthority
     * @param partition 分区号
     * @param idNamespace id的name类型，取得是点 或者 边 或者schema 点的 枚举序号， 根据parttion和枚举序号确定块的起始值。
     * @param idUpperBound  id上线
     * @param renewTimeout
     * @param renewBufferPercentage 获取新的buffer，当前current使用占比。
     */
    public StandardIDPool(IDAuthority idAuthority, int partition, int idNamespace, long idUpperBound, Duration renewTimeout, double renewBufferPercentage) {
        Preconditions.checkArgument(idUpperBound > 0);
        this.idAuthority = idAuthority;
        Preconditions.checkArgument(partition>=0);
        this.partition = partition;
        Preconditions.checkArgument(idNamespace>=0);
        this.idNamespace = idNamespace;
        this.idUpperBound = idUpperBound;
        Preconditions.checkArgument(!renewTimeout.isZero(), "Renew-timeout must be positive");
        this.renewTimeout = renewTimeout;
        Preconditions.checkArgument(renewBufferPercentage>0.0 && renewBufferPercentage<=1.0,"Renew-buffer percentage must be in (0.0,1.0]");
        this.renewBufferPercentage = renewBufferPercentage;

        //创建一个IdPool的时候的初始状态。 块里面的id数为0.
        currentBlock = UNINITIALIZED_BLOCK;
        currentIndex = 0;
        renewBlockIndex = 0;

        nextBlock = null;

        // daemon=true would probably be fine too
        exec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
                        .setDaemon(false)
                        .setNameFormat("JanusGraphID(" + partition + ")("+idNamespace+")[%d]")
                        .build());
        //exec.allowCoreThreadTimeOut(false);
        //exec.prestartCoreThread();
        idBlockFuture = null;

        closeBlockers = new ArrayDeque<>(4);

        closed = false;
    }

    private synchronized void waitForIDBlockGetter() throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        if (null != idBlockFuture) {
            try {
                nextBlock = idBlockFuture.get(renewTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) failed with an exception in %s",
                        partition, idNamespace, sw.stop());
                throw new JanusGraphException(msg, e);
            } catch (TimeoutException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) timed out in %s",
                        partition, idNamespace, sw.stop());
                // Attempt to cancel the renewer
                idBlockGetter.stopRequested();
                if (idAuthority.supportsInterruption()) {
                    idBlockFuture.cancel(true);
                } else {
                    // Attempt to clean one dead element out of closeBlockers every time we append to it
                    if (!closeBlockers.isEmpty()) {
                        Future<?> f = closeBlockers.peek();
                        if (null != f && f.isDone())
                            closeBlockers.remove();
                    }
                    closeBlockers.add(idBlockFuture);
                }
                throw new JanusGraphException(msg, e);
            } catch (CancellationException e) {
                String msg = String.format("ID block allocation on partition(%d)-namespace(%d) was cancelled after %s",
                        partition, idNamespace, sw.stop());
                throw new JanusGraphException(msg, e);
            } finally {
                idBlockFuture = null;
            }
            // Allow InterruptedException to propagate up the stack
        }
    }

    //提交一个异步任务获取下一个block，如果下一个块为null，同步阻塞等待
    private synchronized void nextBlock() throws InterruptedException {
        assert currentIndex == currentBlock.numIds();
        Preconditions.checkState(!closed,"ID Pool has been closed for partition(%s)-namespace(%s) - cannot apply for new id block",
                partition,idNamespace);

        //第一次访问这个block块。
        if (null == nextBlock && null == idBlockFuture) {
            startIDBlockGetter();
        }

        //阻塞等待， 获取到后nextBlock将会被设置值。
        if (null == nextBlock) {
            waitForIDBlockGetter();
        }

        if (nextBlock == ID_POOL_EXHAUSTION)
            throw new IDPoolExhaustedException("Exhausted ID Pool for partition(" + partition+")-namespace("+idNamespace+")");

        currentBlock = nextBlock;
        currentIndex = 0;

        log.debug("ID partition({})-namespace({}) acquired block: [{}]", partition, idNamespace, currentBlock);

        assert currentBlock.numIds()>0;

        nextBlock = null;

        assert RENEW_ID_COUNT>0;
        //计算下次获取block块的索引值。
        renewBlockIndex = Math.max(0,currentBlock.numIds()-Math.max(RENEW_ID_COUNT, Math.round(currentBlock.numIds()*renewBufferPercentage)));
        assert renewBlockIndex<currentBlock.numIds() && renewBlockIndex>=currentIndex;
    }

    /**
     * 每一个PartitionIDPool 都有对应的不同类型的StandardIDPool：
     * 会根据 parttion  与 不同类型的点id 去确定这个block的预申请id。
     * NORMAL_VERTEX：用于vertex id的分配
     * UNMODIFIABLE_VERTEX：用于schema label id的分配
     * RELATION：用于edge id的分配
     *
     * @return
     */
    @Override
    public synchronized long nextID() {
        assert currentIndex <= currentBlock.numIds();

        // 此处涉及两种情况：
        // 1、分区对应的IDPool是第一次被初始化；则currentIndex = 0； currentBlock.numIds() = 0；
        // 2、分区对应的该IDPool不是第一次，但是此次的index正好使用到了current block的最后一个coun
        if (currentIndex == currentBlock.numIds()) {
            try {
                //获取下一个block块的数据。第一次进来，
                // 或者当前block用完了，会进去方法。currentBlock 更换为当前block。
                nextBlock();
            } catch (InterruptedException e) {
                throw new JanusGraphException("Could not renew id block due to interruption", e);
            }
        }

        // 在使用current block的过程中，当current index  ==  renewBlockIndex时，触发double buffer next block的异步获取！！！！
        if (currentIndex == renewBlockIndex) {
            startIDBlockGetter(); //提交任务到索引值。
        }

        long returnId = currentBlock.getId(currentIndex);
        currentIndex++;
        if (returnId >= idUpperBound) throw new IDPoolExhaustedException("Reached id upper bound of " + idUpperBound);
        log.trace("partition({})-namespace({}) Returned id: {}", partition, idNamespace, returnId);
        return returnId;
    }

    @Override
    public synchronized void close() {
        closed=true;
        try {
            waitForIDBlockGetter();
        } catch (InterruptedException e) {
            throw new JanusGraphException("Interrupted while waiting for id renewer thread to finish", e);
        }

        for (Future<?> closeBlocker : closeBlockers) {
            try {
                closeBlocker.get();
            } catch (InterruptedException e) {
                throw new JanusGraphException("Interrupted while waiting for runaway ID renewer task " + closeBlocker, e);
            } catch (ExecutionException e) {
                log.debug("Runaway ID renewer task completed with exception", e);
            }
        }
        exec.shutdownNow();
    }

    private synchronized void startIDBlockGetter() {
        Preconditions.checkArgument(idBlockFuture == null, idBlockFuture);
        if (closed) return; //Don't renew anymore if closed
        //Renew buffer
        //这里创建一个IDBlockGetter的异步任务，返回future设置到idBlockFuture属性中。future返回的是一个IDBlock
        log.debug("Starting id block renewal thread upon {}", currentIndex);
        idBlockGetter = new IDBlockGetter(idAuthority, partition, idNamespace, renewTimeout);
        idBlockFuture = exec.submit(idBlockGetter);
    }

    //重试获取IdBlock。
    private static class IDBlockGetter implements Callable<IDBlock> {

        private final Stopwatch alive;
        private final IDAuthority idAuthority;
        private final int partition;
        private final int idNamespace;
        private final Duration renewTimeout;
        private volatile boolean stopRequested;

        public IDBlockGetter(IDAuthority idAuthority, int partition, int idNamespace, Duration renewTimeout) {
            this.idAuthority = idAuthority;
            this.partition = partition;
            this.idNamespace = idNamespace;
            this.renewTimeout = renewTimeout;
            this.alive = Stopwatch.createStarted();
        }

        private void stopRequested()
        {
            this.stopRequested = true;
        }

        @Override
        public IDBlock call() {
            Stopwatch running = Stopwatch.createStarted();

            try {
                if (stopRequested) {
                    log.debug("Aborting ID block retrieval on partition({})-namespace({}) after " +
                            "graceful shutdown was requested, exec time {}, exec+q time {}",
                            partition, idNamespace, running.stop(), alive.stop());
                    throw new JanusGraphException("ID block retrieval aborted by caller");
                }
                //乐观锁方式获取下一个block的值。
                IDBlock idBlock = idAuthority.getIDBlock(partition, idNamespace, renewTimeout);
                log.debug("Retrieved ID block from authority on partition({})-namespace({}), " +
                          "exec time {}, exec+q time {}",
                          partition, idNamespace, running.stop(), alive.stop());
                Preconditions.checkArgument(idBlock!=null && idBlock.numIds()>0);
                return idBlock;
            } catch (BackendException e) {
                throw new JanusGraphException("Could not acquire new ID block from storage", e);
            } catch (IDPoolExhaustedException e) {
                return ID_POOL_EXHAUSTION;
            }
        }
    }
}
