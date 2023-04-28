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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.*;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.relations.ReassignableRelation;
import org.janusgraph.util.stats.NumberUtil;

import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.database.idassigner.placement.*;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
//分配
@PreInitializeConfigOptions
public class VertexIDAssigner implements AutoCloseable {

    private static final Logger log =
            LoggerFactory.getLogger(VertexIDAssigner.class);

    private static final int MAX_PARTITION_RENEW_ATTEMPTS = 1000;

    public static final ConfigOption<String> PLACEMENT_STRATEGY = new ConfigOption<>(IDS_NS, "placement",
            "Name of the vertex placement strategy or full class name", ConfigOption.Type.MASKABLE, "simple");

    private static final Map<String,String> REGISTERED_PLACEMENT_STRATEGIES = ImmutableMap.of(
            "simple", SimpleBulkPlacementStrategy.class.getName()
    );


    final ConcurrentMap<Integer,PartitionIDPool> idPools;
    final StandardIDPool schemaIdPool;
    final StandardIDPool partitionVertexIdPool;

    private final IDAuthority idAuthority;
    private final IDManager idManager;
    private final IDPlacementStrategy placementStrategy;

    //For StandardIDPool
    private final Duration renewTimeoutMS;
    private final double renewBufferPercentage;

    private final int partitionIdBound;
    private final boolean hasLocalPartitions;

    public VertexIDAssigner(Configuration config, IDAuthority idAuthority, StoreFeatures idAuthFeatures) {
        Preconditions.checkNotNull(idAuthority);
        this.idAuthority = idAuthority;//janusgraph_ids store对应的值。


        int partitionBits = NumberUtil.getPowerOf2(config.get(CLUSTER_MAX_PARTITIONS));
        idManager = new IDManager(partitionBits);
        Preconditions.checkArgument(idManager.getPartitionBound() <= Integer.MAX_VALUE && idManager.getPartitionBound()>0);
        this.partitionIdBound = (int)idManager.getPartitionBound();
        hasLocalPartitions = idAuthFeatures.hasLocalKeyPartition();

        placementStrategy = Backend.getImplementationClass(config, config.get(PLACEMENT_STRATEGY),
                REGISTERED_PLACEMENT_STRATEGIES);
        placementStrategy.injectIDManager(idManager);
        log.debug("Partition IDs? [{}], Local Partitions? [{}]",true,hasLocalPartitions);

        long baseBlockSize = config.get(IDS_BLOCK_SIZE);
        idAuthority.setIDBlockSizer(new SimpleVertexIDBlockSizer(baseBlockSize));

        renewTimeoutMS = config.get(IDS_RENEW_TIMEOUT);
        renewBufferPercentage = config.get(IDS_RENEW_BUFFER_PERCENTAGE);

        idPools = new ConcurrentHashMap<>(partitionIdBound);
        schemaIdPool = new StandardIDPool(idAuthority, IDManager.SCHEMA_PARTITION, PoolType.SCHEMA.getIDNamespace(),
                IDManager.getSchemaCountBound(), renewTimeoutMS, renewBufferPercentage);
        partitionVertexIdPool = new StandardIDPool(idAuthority, IDManager.PARTITIONED_VERTEX_PARTITION, PoolType.PARTITIONED_VERTEX.getIDNamespace(),
                PoolType.PARTITIONED_VERTEX.getCountBound(idManager), renewTimeoutMS, renewBufferPercentage);
        setLocalPartitions(partitionBits);
    }

    private void setLocalPartitionsToGlobal(int partitionBits) {
        placementStrategy.setLocalPartitionBounds(PartitionIDRange.getGlobalRange(partitionBits));
    }

    private void setLocalPartitions(int partitionBits) {
        if (!hasLocalPartitions) {
            setLocalPartitionsToGlobal(partitionBits);
        } else {
            List<PartitionIDRange> partitionRanges = ImmutableList.of();
            try {
                partitionRanges = PartitionIDRange.getIDRanges(partitionBits,idAuthority.getLocalIDPartition());
            } catch (Throwable e) {
                log.error("Could not process local id partitions",e);
            }

            if (!partitionRanges.isEmpty()) {
                log.info("Setting individual partition bounds: {}", partitionRanges);
                placementStrategy.setLocalPartitionBounds(partitionRanges);
            } else {
                setLocalPartitionsToGlobal(partitionBits);
            }
        }
    }

    public IDManager getIDManager() {
        return idManager;
    }

    public synchronized void close() {
        schemaIdPool.close();
        for (PartitionIDPool pool : idPools.values()) {
            pool.close();
        }
        idPools.clear();
    }

    public void assignID(InternalRelation relation) {
        assignID(relation, null);
    }

    public void assignID(InternalVertex vertex, VertexLabel label) {
        Preconditions.checkArgument(vertex!=null && label!=null);
        assignID(vertex,getVertexIDType(label));
    }


    private void assignID(InternalElement element, IDManager.VertexIDType vertexIDType) {
        //重试获取点。
        for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS; attempt++) {
            //1. 先确定用那个parttion
            long partitionID = -1;
            if (element instanceof JanusGraphSchemaVertex) { //schema 类型节点。分区就是0
                partitionID = IDManager.SCHEMA_PARTITION;
            } else if (element instanceof JanusGraphVertex) { //正常逻辑，分区就是
                if (vertexIDType== IDManager.VertexIDType.PartitionedVertex) //按照点切割的点label，分区是1， 这类型的点的边特别多，对这种点特殊处理了。
                    partitionID = IDManager.PARTITIONED_VERTEX_PARTITION;
                else
                    partitionID = placementStrategy.getPartition(element); //获取到分区数，简单的获取逻辑就是32 之间随机数。
            } else if (element instanceof InternalRelation) { // 属性 + 边
                InternalRelation relation = (InternalRelation)element;
                if (attempt < relation.getLen()) { //On the first attempts, try to use partition of incident vertices
                    InternalVertex incident = relation.getVertex(attempt);
                    Preconditions.checkArgument(incident.hasId());
                    //获取对应节点已有的分区id
                    if (!IDManager.VertexIDType.PartitionedVertex.is(incident.longId()) || relation.isProperty()) {
                        partitionID = getPartitionID(incident);
                    } else {
                        continue;
                    }
                } else {
                    //随机获取一个分区号
                    partitionID = placementStrategy.getPartition(element);
                }
            }

            //2. 正式分配id ，根据parttion 和 节点类型
            try {
                assignID(element, partitionID, vertexIDType);
            } catch (IDPoolExhaustedException e) {
                continue; //try again on a different partition
            }
            assert element.hasId();

            /*
              The next block of code checks the added the relation for partitioned vertices as either end point. If such exists,
              we might have to assign the relation to a different representative of that partitioned vertex using the following logic:
              1) Properties are always assigned to the canonical representative
              2) Edges are assigned to the partition block of the non-partitioned vertex
               2a) unless the edge is unique in the direction away from the partitioned vertex in which case its assigned to the canonical representative
               2b) if both end vertices are partitioned, it is assigned to the partition to which the edge id hashes
             */
            //Check if we should assign a different representative of a potential partitioned vertex
            if (element instanceof InternalRelation) {
                InternalRelation relation = (InternalRelation)element;
                if (relation.isProperty() && isPartitionedAt(relation,0)) {
                    //Always assign properties to the canonical representative of a partitioned vertex
                    InternalVertex vertex = relation.getVertex(0);
                    ((ReassignableRelation)relation).setVertexAt(0,vertex.tx().getInternalVertex(idManager.getCanonicalVertexId(vertex.longId())));
                } else if (relation.isEdge()) {
                    for (int pos = 0; pos < relation.getArity(); pos++) {
                        if (isPartitionedAt(relation, pos)) {
                            InternalVertex incident = relation.getVertex(pos);
                            long newPartition;
                            int otherPosition = (pos+1)%2;
                            if (((InternalRelationType)relation.getType()).multiplicity().isUnique(EdgeDirection.fromPosition(pos))) {
                                //If the relation is unique in the direction, we assign it to the canonical vertex...
                                newPartition = idManager.getPartitionId(idManager.getCanonicalVertexId(incident.longId()));
                            } else if (!isPartitionedAt(relation,otherPosition)) {
                                //...else, we assign it to the partition of the non-partitioned vertex...
                                newPartition = getPartitionID(relation.getVertex(otherPosition));
                            } else {
                                //...and if such does not exists (i.e. both end vertices are partitioned) we use the hash of the relation id
                                newPartition = idManager.getPartitionHashForId(relation.longId());
                            }
                            if (idManager.getPartitionId(incident.longId())!=newPartition) {
                                ((ReassignableRelation)relation).setVertexAt(pos,incident.tx().getOtherPartitionVertex(incident, newPartition));
                            }
                        }
                    }
                }
            }
            return;
        }
        throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
    }

    private boolean isPartitionedAt(InternalRelation relation, int position) {
        return idManager.isPartitionedVertex(relation.getVertex(position).longId());
    }

    public void assignIDs(Iterable<InternalRelation> addedRelations) {
        if (!placementStrategy.supportsBulkPlacement()) {
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        assignID(vertex, getVertexIDType(vertex));
                    }
                }
                assignID(relation);
            }
        } else {
            //2) only assign ids to (user) vertices
            Map<InternalVertex, PartitionAssignment> assignments = new HashMap<>();
            for (InternalRelation relation : addedRelations) {
                for (int i = 0; i < relation.getArity(); i++) {
                    InternalVertex vertex = relation.getVertex(i);
                    if (!vertex.hasId()) {
                        assert !(vertex instanceof JanusGraphSchemaVertex); //Those are assigned ids immediately in the transaction
                        if (vertex.vertexLabel().isPartitioned())
                            assignID(vertex, getVertexIDType(vertex)); //Assign partitioned vertex ids immediately
                        else
                            assignments.put(vertex, PartitionAssignment.EMPTY);
                    }
                }
            }
            log.trace("Bulk id assignment for {} vertices", assignments.size());
            for (int attempt = 0; attempt < MAX_PARTITION_RENEW_ATTEMPTS && (assignments != null && !assignments.isEmpty()); attempt++) {
                placementStrategy.getPartitions(assignments);
                Map<InternalVertex, PartitionAssignment> leftOvers = null;
                Iterator<Map.Entry<InternalVertex, PartitionAssignment>> iterator = assignments.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<InternalVertex, PartitionAssignment> entry = iterator.next();
                    try {
                        assignID(entry.getKey(), entry.getValue().getPartitionID(), getVertexIDType(entry.getKey()));
                        Preconditions.checkArgument(entry.getKey().hasId());
                    } catch (IDPoolExhaustedException e) {
                        if (leftOvers == null) leftOvers = new HashMap<>();
                        leftOvers.put(entry.getKey(), PartitionAssignment.EMPTY);
                        break;
                    }
                }
                if (leftOvers != null) {
                    while (iterator.hasNext()) leftOvers.put(iterator.next().getKey(), PartitionAssignment.EMPTY);
                    log.debug("Exhausted ID Pool in bulk assignment. Left-over vertices {}", leftOvers.size());
                }
                assignments = leftOvers;
            }
            if (assignments != null && !assignments.isEmpty())
                throw new IDPoolExhaustedException("Could not find non-exhausted partition ID Pool after " + MAX_PARTITION_RENEW_ATTEMPTS + " attempts");
            //3) assign ids to relations
            for (InternalRelation relation : addedRelations) {
                assignID(relation);
            }
        }
    }

    private long getPartitionID(final InternalVertex v) {
        long vid = v.longId();
        if (IDManager.VertexIDType.Schema.is(vid)) return IDManager.SCHEMA_PARTITION;
        else return idManager.getPartitionId(vid);
    }

    /**
     *
     * @param element
     * @param partitionIDl
     * @param userVertexIDType
     */
    private void assignID(final InternalElement element, final long partitionIDl, final IDManager.VertexIDType userVertexIDType) {
        Preconditions.checkNotNull(element);
        Preconditions.checkArgument(!element.hasId());
        Preconditions.checkArgument((element instanceof JanusGraphRelation) ^ (userVertexIDType!=null));
        Preconditions.checkArgument(partitionIDl >= 0 && partitionIDl < partitionIdBound, partitionIDl);
        final int partitionID = (int) partitionIDl;

        long count;
        if (element instanceof JanusGraphSchemaVertex) { //schema点的分区号是0
            Preconditions.checkArgument(partitionID==IDManager.SCHEMA_PARTITION);
            count = schemaIdPool.nextID();
            // 配置的热点节点，类似于`makeVertexLabel('product').partition()`的处理
        } else if (userVertexIDType==IDManager.VertexIDType.PartitionedVertex) {//相当于是个热点数据集的处理，对这种label类型的顶点。
            Preconditions.checkArgument(partitionID==IDManager.PARTITIONED_VERTEX_PARTITION);
            Preconditions.checkArgument(partitionVertexIdPool!=null);
            count = partitionVertexIdPool.nextID();
        } else {
            PartitionIDPool partitionPool = idPools.get(partitionID);
            if (partitionPool == null) { //该分区对应的IDPool不存在的话，
                // 在PartitionIDPool中包含多种类型对应的StandardIDPool类型
                //StandardIDPool中包含对应的block和count信息。
                partitionPool = new PartitionIDPool(partitionID, idAuthority, idManager, renewTimeoutMS, renewBufferPercentage);
                idPools.putIfAbsent(partitionID,partitionPool);
                partitionPool = idPools.get(partitionID);
            }
            Preconditions.checkNotNull(partitionPool);
            if (partitionPool.isExhausted()) { //该分区id 用完了。 抛出异常重试 换下个分区。
                placementStrategy.exhaustedPartition(partitionID);
                throw new IDPoolExhaustedException("Exhausted id pool for partition: " + partitionID);
            }
            IDPool idPool;
            if (element instanceof JanusGraphRelation) {
                //关系类型的节点idPool。
                idPool = partitionPool.getPool(PoolType.RELATION);
            } else {
                //点类型的idpool。
                Preconditions.checkArgument(userVertexIDType!=null);
                //TODO: 此位置，获取池，根据元素类型。
                idPool = partitionPool.getPool(PoolType.getPoolTypeFor(userVertexIDType));
            }
            try {
                //获取不同类型下idpool中的id值。
                count = idPool.nextID();
                partitionPool.accessed();
            } catch (IDPoolExhaustedException e) {
                log.debug("Pool exhausted for partition id {}", partitionID);
                placementStrategy.exhaustedPartition(partitionID);
                partitionPool.exhaustedIdPool();
                throw e;
            }
        }

        long elementId;
        if (element instanceof InternalRelation) { //为边或者属性组成一个边id
            elementId = idManager.getRelationID(count, partitionID);
        } else if (element instanceof PropertyKey) { //为属性点 组成一个分区id
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.UserPropertyKey,count);
        } else if (element instanceof EdgeLabel) { //为 edgeLabel点 组成一个分区id。
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.UserEdgeLabel, count);
        } else if (element instanceof VertexLabel) { //为 vertexLabel点组成一个分区id
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.VertexLabel, count);
        } else if (element instanceof JanusGraphSchemaVertex) { // 有用？
            elementId = IDManager.getSchemaId(IDManager.VertexIDType.GenericSchemaType,count);
        } else {  //为普通点组成一个分区id。
            elementId = idManager.getVertexID(count, partitionID, userVertexIDType);
        }

        Preconditions.checkArgument(elementId >= 0);
        element.setId(elementId);
    }

    private static IDManager.VertexIDType getVertexIDType(VertexLabel vertexLabel) {
        if (vertexLabel.isPartitioned()) {
            return IDManager.VertexIDType.PartitionedVertex;
        } else if (vertexLabel.isStatic()) {
            return IDManager.VertexIDType.UnmodifiableVertex;
        } else {
            return IDManager.VertexIDType.NormalVertex;
        }
    }

    private static IDManager.VertexIDType getVertexIDType(JanusGraphVertex v) {
        return getVertexIDType(v.vertexLabel());
    }

    private class SimpleVertexIDBlockSizer implements IDBlockSizer {

        private final long baseBlockSize;

        SimpleVertexIDBlockSizer(final long size) {
            Preconditions.checkArgument(size > 0 && size < Integer.MAX_VALUE);
            this.baseBlockSize = size;
        }

        @Override
        public long getBlockSize(int idNamespace) {
            switch (PoolType.getPoolType(idNamespace)) {
                case NORMAL_VERTEX:
                    return baseBlockSize;
                case UNMODIFIABLE_VERTEX:
                    return Math.max(10,baseBlockSize/10);
                case PARTITIONED_VERTEX:
                    return Math.max(10,baseBlockSize/100);
                case RELATION:
                    return baseBlockSize * 8;
                case SCHEMA:
                    return 50;

                default:
                    throw new IllegalArgumentException("Unrecognized pool type");
            }
        }

        @Override
        public long getIdUpperBound(int idNamespace) {
            return PoolType.getPoolType(idNamespace).getCountBound(idManager);
        }
    }

    private enum PoolType {
       // UNMODIFIABLE_VERTEX 用于schema label id的分配
        NORMAL_VERTEX, UNMODIFIABLE_VERTEX, PARTITIONED_VERTEX, RELATION, SCHEMA;

        public int getIDNamespace() {
            return ordinal();
        }

        public long getCountBound(IDManager idManager) {
            switch (this) {
                case NORMAL_VERTEX:
                case UNMODIFIABLE_VERTEX:
                case PARTITIONED_VERTEX:
                    return idManager.getVertexCountBound();
                case RELATION: return idManager.getRelationCountBound();
                case SCHEMA: return IDManager.getSchemaCountBound();
                default: throw new AssertionError("Unrecognized type: " + this);
            }
        }

        public boolean hasOnePerPartition() {
            switch(this) {
                case NORMAL_VERTEX:
                case UNMODIFIABLE_VERTEX:
                case RELATION:
                    return true;
                default: return false;
            }
        }

        public static PoolType getPoolTypeFor(IDManager.VertexIDType idType) {
            if (idType==IDManager.VertexIDType.NormalVertex) return NORMAL_VERTEX;
            else if (idType== IDManager.VertexIDType.UnmodifiableVertex) return UNMODIFIABLE_VERTEX;
            else if (idType== IDManager.VertexIDType.PartitionedVertex) return PARTITIONED_VERTEX;
            else if (IDManager.VertexIDType.Schema.isSubType(idType)) return SCHEMA;
            else throw new IllegalArgumentException("Invalid id type: " + idType);
        }

        public static PoolType getPoolType(int idNamespace) {
            Preconditions.checkArgument(idNamespace>=0 && idNamespace<values().length);
            return values()[idNamespace];
        }

    }

    private static class PartitionIDPool extends EnumMap<PoolType,IDPool> {

        private volatile long lastAccess;
        private volatile boolean exhausted;

        // 对每个需要获取唯一点的元素，有点id，边ID 和 schemal label的id。 给每一个类型创建一个StandardIDPool。 每个类型下IDPool都是独立的，独立的
        //去获取自己类型的block。idNamespace
        PartitionIDPool(int partitionID, IDAuthority idAuthority, IDManager idManager, Duration renewTimeoutMS, double renewBufferPercentage) {
            super(PoolType.class);
            //给需要获取该分区内的id点或者边类型 都创建一个StandardIDPool，去分类型获取id。
            for (PoolType type : PoolType.values()) {
                if (!type.hasOnePerPartition()) continue;
                //为 正常点， 不正常点 与边 各创建一个IDPool，schema 和 parttion点不用创建了。
                put(type,new StandardIDPool(idAuthority, partitionID, type.getIDNamespace(), type.getCountBound(idManager), renewTimeoutMS, renewBufferPercentage));
            }
        }

        /**
         *     /**
         *      * 每一个PartitionIDPool 都有对应的不同类型的StandardIDPool：
         *      *
         *      * NORMAL_VERTEX：用于vertex id的分配
         *      * UNMODIFIABLE_VERTEX：用于schema label id的分配
         *      * RELATION：用于edge id的分配
         *      *
         *      * @return
         *      */

        public IDPool getPool(PoolType type) {
            Preconditions.checkArgument(!exhausted && type.hasOnePerPartition());
            return super.get(type);
        }

        public void close() {
            for (IDPool pool : values()) pool.close();
            super.clear();
        }

        public void exhaustedIdPool() {
            exhausted = true;
            close();
        }

        public boolean isExhausted() {
            return exhausted;
        }

        public void accessed() {
            lastAccess = System.currentTimeMillis();
        }

        public long getLastAccess() {
            return lastAccess;
        }

    }



}
