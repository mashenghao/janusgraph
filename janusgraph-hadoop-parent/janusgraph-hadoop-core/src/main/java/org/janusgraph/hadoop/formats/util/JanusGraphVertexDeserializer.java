//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.janusgraph.hadoop.formats.util;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.janusgraph.core.*;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.hadoop.formats.util.input.SystemTypeInspector;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetup;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class JanusGraphVertexDeserializer implements AutoCloseable {

    private final JanusGraphHadoopSetup setup;
    private final TypeInspector typeManager;
    private final SystemTypeInspector systemTypes;
    private final IDManager idManager;

    private static final Logger log =
        LoggerFactory.getLogger(JanusGraphVertexDeserializer.class);

    public JanusGraphVertexDeserializer(final JanusGraphHadoopSetup setup) {
        this.setup = setup;
        this.typeManager = setup.getTypeInspector();
        this.systemTypes = setup.getSystemTypeInspector();
        this.idManager = setup.getIDManager();
    }

    private static Boolean isLoopAdded(Vertex vertex, String label) {
        Iterator<Vertex> adjacentVertices = vertex.vertices(Direction.BOTH, label);

        while (adjacentVertices.hasNext()) {
            Vertex adjacentVertex = adjacentVertices.next();

            if (adjacentVertex.equals(vertex)) {
                return true;
            }
        }

        return false;
    }

    // Read a single row from the edgestore and create a TinkerVertex corresponding to the row
    // The neighboring vertices are represented by DetachedVertex instances
    public TinkerVertex readHadoopVertex(final StaticBuffer key, Iterable<Entry> entries, GraphFilter filter) {

        // Convert key to a vertex ID 根据rowkey解析vertexid
        final long vertexId = idManager.getKeyID(key);
        Preconditions.checkArgument(vertexId > 0);

        // Partitioned vertex handling
        if (idManager.isPartitionedVertex(vertexId)) {
            Preconditions.checkState(setup.getFilterPartitionedVertices(),
                "Read partitioned vertex (ID=%s), but partitioned vertex filtering is disabled.", vertexId);
            log.debug("Skipping partitioned vertex with ID {}", vertexId);
            return null;
        }

        // Create TinkerVertex
        TinkerGraph tg = TinkerGraph.open();

        TinkerVertex tv = null;

        //节点的label id是存在edge的column中的，从edge列簇中遍历寻找label id。点的label的column的lable id项=2
        // Iterate over edgestore columns to find the vertex's label relation
        for (final Entry data : entries) {
            RelationReader relationReader = setup.getRelationReader(vertexId);
            final RelationCache relation = relationReader.parseRelation(data, false, typeManager);
            if (systemTypes.isVertexLabelSystemType(relation.typeId)) {
                // Found vertex Label，从边列簇中找到了typeid是点标签的边。
                // 这是把点label的schema定义是一个图节点，该label下的数据点之间有个relation，relation的OtherVertexId是label点。
                long vertexLabelId = relation.getOtherVertexId();
                VertexLabel vl = typeManager.getExistingVertexLabel(vertexLabelId);
                if (filter != null && filter.isHasVertexLabelFilter() && !filter.getVertexLabelFilter().test(vl.name())) {
                    return null;
                }
                // Create TinkerVertex with this label
                tv = getOrCreateVertex(vertexId, vl.name(), tg);
//                break;
            }
        }

        // Added this following testing
        if (null == tv) {
            tv = getOrCreateVertex(vertexId, null, tg);
        }

        Preconditions.checkNotNull(tv, "Unable to determine vertex label for vertex with ID %s", vertexId);

        // Iterate over and decode edgestore columns (relations) on this vertex
        //去找边信息了或者点的属性信息。
        for (final Entry data : entries) {
            try {
                if (filter != null && filter.allowNoEdges() && filter.allowNoProperties()) {
                    break;
                }
                RelationReader relationReader = setup.getRelationReader(vertexId);
                final RelationCache relation = relationReader.parseRelation(data, false, typeManager);

                if (systemTypes.isSystemType(relation.typeId)) continue; //Ignore system types
                final RelationType type = typeManager.getExistingRelationType(relation.typeId);
                if (((InternalRelationType) type).isInvisibleType()) continue; //Ignore hidden types

                // Decode and create the relation (edge or property)
                if (type.isPropertyKey()) { //typeId是个PropertyKey，解析为中心的属性值。
                    if (filter != null && (filter.allowNoProperties() || filter.checkPropertyLegality(type.name()).negative()))
                        continue;
                    // Decode property
                    Object value = relation.getValue();
                    Preconditions.checkNotNull(value);
                    VertexProperty.Cardinality card = getPropertyKeyCardinality(type.name());
                    tv.property(card, type.name(), value, T.id, relation.relationId);
                } else {
                    assert type.isEdgeLabel();
                    if (filter != null && filter.allowNoEdges()) continue;
                    // Partitioned vertex handling
                    if (idManager.isPartitionedVertex(relation.getOtherVertexId())) {
                        Preconditions.checkState(setup.getFilterPartitionedVertices(),
                            "Read edge incident on a partitioned vertex, but partitioned vertex filtering is disabled.  " +
                                "Relation ID: %s.  This vertex ID: %s.  Other vertex ID: %s.  Edge label: %s.",
                            relation.relationId, vertexId, relation.getOtherVertexId(), type.name());
                        log.debug("Skipping edge with ID {} incident on partitioned vertex with ID {} (and nonpartitioned vertex with ID {})",
                            relation.relationId, relation.getOtherVertexId(), vertexId);
                        continue;
                    }

                    // Decode edge
                    TinkerEdge te;

                    // We don't know the label of the other vertex, but one must be provided
                    TinkerVertex adjacentVertex = getOrCreateVertex(relation.getOtherVertexId(), null, tg);

                    // handle self-loop edges
                    if (tv.equals(adjacentVertex) && isLoopAdded(tv, type.name())) {
                        continue;
                    }

                    if (relation.direction.equals(Direction.IN)) {
                        if (filter != null && filter.checkEdgeLegality(Direction.IN, type.name()).negative()) continue;
                        te = (TinkerEdge) adjacentVertex.addEdge(type.name(), tv, T.id, relation.relationId);
                    } else if (relation.direction.equals(Direction.OUT)) {
                        if (filter != null && filter.checkEdgeLegality(Direction.OUT, type.name()).negative()) continue;
                        te = (TinkerEdge) tv.addEdge(type.name(), adjacentVertex, T.id, relation.relationId);
                    } else {
                        throw new RuntimeException("Direction.BOTH is not supported");
                    }

                    if (relation.hasProperties()) {
                        // Load relation properties
                        for (final LongObjectCursor<Object> next : relation) {
                            assert next.value != null;
                            RelationType rt = typeManager.getExistingRelationType(next.key);
                            if (filter != null && filter.checkPropertyLegality(rt.name()).negative()) continue;
                            if (rt.isPropertyKey()) {
                                te.property(rt.name(), next.value);
                            } else {
                                throw new RuntimeException("Metaedges are not supported");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /*Since we are filtering out system relation types, we might end up with vertices that have no incident relations.
         This is especially true for schema vertices. Those are filtered out.     */
        if (!tv.edges(Direction.BOTH).hasNext() && !tv.properties().hasNext()) {
            if (schemaVertices(tv)) {
                return null;
            }
            log.trace("Vertex {} has no relations", vertexId);
            return tv;
        }
        return tv;
    }

    public boolean schemaVertices(Vertex tv) {
        return "vertex".equals(tv.label());
    }

    public TinkerVertex getOrCreateVertex(final long vertexId, final String label, final TinkerGraph tg) {
        TinkerVertex v;

        try {
            v = (TinkerVertex) tg.vertices(vertexId).next();
        } catch (NoSuchElementException e) {
            if (null != label) {
                v = (TinkerVertex) tg.addVertex(T.label, label, T.id, vertexId);
            } else {
                v = (TinkerVertex) tg.addVertex(T.id, vertexId);
            }
        }

        return v;
    }

    private VertexProperty.Cardinality getPropertyKeyCardinality(String name) {
        RelationType rt = typeManager.getRelationType(name);
        if (null == rt || !rt.isPropertyKey())
            return VertexProperty.Cardinality.single;
        PropertyKey pk = typeManager.getExistingPropertyKey(rt.longId());
        switch (pk.cardinality()) {
            case SINGLE: return VertexProperty.Cardinality.single;
            case LIST: return VertexProperty.Cardinality.list;
            case SET: return VertexProperty.Cardinality.set;
            default: throw new IllegalStateException("Unknown cardinality " + pk.cardinality());
        }
    }

    public void close() {
        setup.close();
    }
}
