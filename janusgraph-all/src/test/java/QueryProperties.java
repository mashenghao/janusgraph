import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.vertices.StandardVertex;

/**
 * @author mahao
 * @date 2022/08/21
 */
public class QueryProperties {

    public static void main(String[] args) {
        Configuration conf = new BaseConfiguration();
        conf.addProperty("storage.backend", "hbase");
        conf.addProperty("storage.hostname", "cs3");
        conf.addProperty("storage.port", "2181");
        conf.addProperty("storage.hbase.table", "product_2");
        StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(conf);

        StandardVertex vertex = (StandardVertex) graph.vertices(100L).next();
        vertex.property("aa");

        graph.query();

        //返回一个图中心查询实例
        graph.edgeQuery(vertex.longId(), null, null);
        JanusGraphQuery graphQuery = graph.query();

        // 返回一个点中心查询的实例，可以基于这个点构造查询条件，查找属性 边等信息。
        //JanusGraphVertexQuery是父类。
        VertexCentricQueryBuilder vertexQuery = vertex.query();

        vertexQuery.properties();


    }
}
