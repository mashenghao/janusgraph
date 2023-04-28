import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.RandomUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author mahao
 * @date 2022/08/18
 */
public class CreateVertex {

    public static void main(String[] args) throws InterruptedException {
        Configuration conf = new BaseConfiguration();
        conf.addProperty("storage.backend", "hbase");
        conf.addProperty("storage.hostname", "cs3");
        conf.addProperty("storage.port", "2181");
        conf.addProperty("storage.hbase.table", "janus2");
        StandardJanusGraph graph = (StandardJanusGraph) JanusGraphFactory.open(conf);

//              makeSchema(graph);
        //      makeData2(graph);
//       query(graph);
        makeData(graph);

        graph.close();
    }

    public static void makeSchema(StandardJanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();
        mgmt.makePropertyKey("name").dataType(String.class).make();
        mgmt.makePropertyKey("addr").dataType(String.class).make();
        mgmt.makePropertyKey("ename").dataType(String.class).make();
        mgmt.makePropertyKey("eaddr").dataType(String.class).make();
        mgmt.makePropertyKey("estr").dataType(String.class).make();
        mgmt.makeVertexLabel("user").make();
        mgmt.makeEdgeLabel("trans").make();
        mgmt.commit();
    }

    public static void query(StandardJanusGraph graph) {
        JanusGraphVertex next = graph.query().has("name", "张三").vertices().iterator().next();
        Iterator<Edge> edges = next.edges(Direction.BOTH);
        while (edges.hasNext()) {
            System.out.println(edges.next());
        }
    }


    public static void makeData2(StandardJanusGraph graph) {
        JanusGraphVertex vertex1 = graph.addVertex(T.label, "user", "name", "张三", "addr", "北京时");
        JanusGraphVertex vertex2 = graph.addVertex(T.label, "user", "name", "李四", "addr", "100");
        vertex1.addEdge("trans", vertex2, "ename", "f3属性", "eaddr", "f4属性", "estr", "属性5");
        graph.tx().commit();
    }

    public static void makeData(StandardJanusGraph graph) {
        List<String> sts = new ArrayList<>();
        sts.add("是中国的语言文字。特指汉族的语言文字，即汉语和汉字在汉字文化圈和海外华人社区中，中文也被称为华文、汉文2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实中文（汉语）有标准语和方言之分，其标准语即汉语普通话，是规范后的汉民族共同语 [1]  ，也是中国的国家通用语言");
        sts.add("现存最早可识的汉字是殷商的甲骨文和稍后的金文，在西周时演变成籀文，到秦朝发展出小篆和秦隶，至汉魏时隶书盛行，到了汉末隶书隶变楷化为正楷，楷书盛行于魏晋南北朝。现代汉字是指楷化后的汉字正楷字形，包括繁体中文和简体中文。2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("截至2020年底，全球共有180多个国家和地区开展中文教育，70多个国家将中文纳入国民教育体系，外国正在学习中文的人数超过2000万，累计学习和使用中文的人数接近2亿2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("2021年1月25日起，中文正式成为联合国世界旅游组织(英文简称UNWTO)官方语言。2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("中文特指汉语言文字或汉语言文学。由于民间“语（言）文（字）”两个概念不分2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("人社区中，中文也被称为汉文、华文。现代汉语（普通话）是世界上使用人数最多的语言2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("中文的书写体系，即是记录汉民族语言的书面符号体系2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("体系得以形成和发展的基础。后来的演变经历了几千年的漫长历程2001年1月1日起，中国施行《中华人民共和国国家通用语言文字法》，该法确立了普通话和规范汉字作为中国通用语言文字的法律地位。 [6]  中文（汉字）的使用人数在17亿以上，范围包括中国全境（大陆、港澳、台湾）和东亚、东南亚等汉字文化圈地区。 [7-8]  2021年7月1日起，作为国家语委语言文字规范，《国际中文教育中文水平等级标准》将正式实");
        sts.add("aaaa");
        int size = 10;

        List<Vertex> list = new ArrayList<>();
        int count = 0;
        int star = 0;
        int end = 50;
        for (int i = star; i < end; i++) {
            JanusGraphVertex vertex = graph.addVertex(T.label, "user", "name", "user-" + i, "addr", sts.get(RandomUtils.nextInt(0, 10)));
            list.add(vertex);
            if (i % 100 == 0) {
                System.out.println(count++);
                graph.tx().commit();
            }
        }

        for (int i = 0; i < list.size(); i++) {
            Vertex vertex = list.get(i);
            int i1 = RandomUtils.nextInt(10, 100);
            if (i % 100 == 0) {
                i1 = 5000;
            }
            for (int j = 0; j < i1; j++) {
                vertex.addEdge("trans", list.get(RandomUtils.nextInt(0, end - star - 1)), "ename", sts.get(RandomUtils.nextInt(0, 10)), "eaddr", sts.get(RandomUtils.nextInt(0, 10)), "estr", sts.get(RandomUtils.nextInt(0, 10)));
            }
            System.out.println(count++);
            if (i % 100 == 0) {
                graph.tx().commit();
            }
        }

        graph.tx().commit();
    }
}
