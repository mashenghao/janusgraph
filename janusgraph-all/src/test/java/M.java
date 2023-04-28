import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.hadoop.formats.util.HadoopInputFormat;
import org.janusgraph.hadoop.formats.util.JanusGraphVertexDeserializer;
import org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetup;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import static org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetupCommon.SETUP_CLASS_NAME;
import static org.janusgraph.hadoop.formats.util.input.JanusGraphHadoopSetupCommon.SETUP_PACKAGE_PREFIX;

/**
 * @author mahao
 * @date 2022/08/15
 */
public class M {

    private static final HadoopInputFormat.RefCountedCloseable<JanusGraphVertexDeserializer> refCounter;

    static {
        refCounter = new HadoopInputFormat.RefCountedCloseable<>((conf) -> {
            final String janusgraphVersion = "current";

            String className = SETUP_PACKAGE_PREFIX + janusgraphVersion + SETUP_CLASS_NAME;

            JanusGraphHadoopSetup ts = ConfigurationUtil.instantiate(
                className, new Object[]{conf}, new Class[]{Configuration.class});

            return new JanusGraphVertexDeserializer(ts);
        });
    }

    public static void main(String[] args) throws Exception {
        Configuration cfg = HBaseConfiguration.create();
        //在设置配置时，最好将ZooKeeper的三个节点都配置，否则性能会受影响（也可以写成其IP地址形式）
        cfg.set("hbase.zookeeper.quorum", "cs3");
        cfg.set("hbase.zookeeper.property.clientPort", "2181");

        cfg.set("janusgraphmr.ioformat.conf.storage.backend", "hbase");
        cfg.set("janusgraphmr.ioformat.conf.storage.hostname", "cs3");
        cfg.set("janusgraphmr.ioformat.conf.storage.port", "2181");
        cfg.set("janusgraphmr.ioformat.conf.storage.hbase.table", "product_2");
        refCounter.setBuilderConfiguration(cfg);

        Connection conn = ConnectionFactory.createConnection(cfg);
        Table table = conn.getTable(TableName.valueOf("product_2"));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("e"));
//        scan.setMaxResultsPerColumnFamily(5);
        ResultScanner rs = table.getScanner(scan);
        Result result;
        while ((result = rs.next()) != null) {
            byte[] rowkey = result.getRow();


            //cf -> 列值
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
            StaticArrayBuffer vertexArrat = StaticArrayBuffer.of(rowkey);
            //边
            List<Entry> edges = new ArrayList<>(map.size());
            map.get(Bytes.toBytes("e")).forEach((k, v) -> {
                byte[] val = (byte[]) (((NavigableMap) v).lastEntry().getValue());
                Entry edgeEntry = StaticArrayEntry.of(new StaticArrayBuffer(k), new StaticArrayBuffer(val));
                edges.add(edgeEntry);
            });

            TinkerVertex tinkerVertex = refCounter.acquire().readHadoopVertex(vertexArrat, edges, null);
            if (tinkerVertex != null) {
                System.out.println(tinkerVertex.property("value").value() + "   " + IteratorUtils.count(tinkerVertex.edges(Direction.BOTH)));
                System.out.println("vertexId -> " + toStr(rowkey));
            } else {
                System.out.println("invId -> " + toStr(rowkey));
            }
        }

        refCounter.release();
        table.close();
    }

    public static String toStr(byte[] bytes) {
        if (bytes == null)
            return null;
        StringBuffer sb = new StringBuffer();
        for (byte aByte : bytes) {
            sb.append(toBin(aByte)).append(" ");
        }
        return sb.toString();
    }

    public static String toBin(byte b) {
        if (b == 0) {
            return "0";
        }
        char[] chars = new char[8];
        int i = 7;
        while (b != 0 && i >= 0) {
            chars[i] = (b & 1) == 0 ? '0' : '1';
            b = (byte) (b >> 1);
            i--;
        }

        return new String(chars, i + 1, 7 - i);
    }

    public static void main2(String[] args) {
        //11
        System.out.println(toBin((byte) 0));
        System.out.println(toBin((byte) 1));
        System.out.println(toBin((byte) 3));
        System.out.println(toBin((byte) 127));
        System.out.println(toBin((byte) -128));
    }

    @Test
    public void testRegionSplit() {
        int regionCount = 3;
        System.out.println(toStr(getStartKey(regionCount)) + "\n" + toStr(getEndKey(regionCount)));
        System.out.println((int) (((1L << 32) - 1L) / regionCount) + "\n" + (int) (((1L << 32) - 1L) / regionCount * (regionCount - 1)));
        int i = Bytes.compareTo(getStartKey(regionCount), getEndKey(regionCount));
        int compare = Integer.compare(1, 2);
        System.out.println(compare);
        System.out.println(i);
        System.out.println(1L << 32);
    }

    /**
     * 得到的是startkey，z^32个数，第一个结尾是2^32/count，开始是2^32-1 * regioncount-1
     * @param regionCount
     * @return
     */
    private byte[] getStartKey(int regionCount) {
        ByteBuffer regionWidth = ByteBuffer.allocate(4);
        regionWidth.putInt((int) (((1L << 32) - 1L) / regionCount)).flip();
        return StaticArrayBuffer.of(regionWidth).getBytes(0, 4);
    }

    /**
     * Companion to {@link #getStartKey(int)}. See its javadoc for details.
     */
    private byte[] getEndKey(int regionCount) {
        ByteBuffer regionWidth = ByteBuffer.allocate(4);
        regionWidth.putInt((int) (((1L << 32) - 1L) / regionCount * (regionCount - 1))).flip();
        return StaticArrayBuffer.of(regionWidth).getBytes(0, 4);
    }
}
