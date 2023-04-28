package org.janusgraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author: mahao
 * @date: 2021/4/9
 */
public class hbasep {

    public static void main(String[] args) throws IOException {
        // 取得一个数据库连接的配置参数对象
        Configuration conf = HBaseConfiguration.create();

        // 设置连接参数：HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "10.100.2.110,10.100.2.120,10.100.2.130");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // 取得一个数据库连接对象
        Connection connection = ConnectionFactory.createConnection(conf);

        // 取得一个数据库元数据操作对象
        Admin admin = connection.getAdmin();
        System.out.println("---------------查询整表数据 START-----------------");
        HTableDescriptor tableDescriptor = new
            HTableDescriptor(TableName.valueOf("abc:ct7"));

        // Adding column families to table descriptor  设置列族名（可设置多个）
        tableDescriptor.addFamily(new HColumnDescriptor("a"));
        tableDescriptor.addFamily(new HColumnDescriptor("s"));
        tableDescriptor.addFamily(new HColumnDescriptor("f"));
        tableDescriptor.addFamily(new HColumnDescriptor("g"));
        tableDescriptor.addFamily(new HColumnDescriptor("h"));
        tableDescriptor.addFamily(new HColumnDescriptor("j"));
        tableDescriptor.addFamily(new HColumnDescriptor("q"));
        tableDescriptor.addFamily(new HColumnDescriptor("w"));
        tableDescriptor.addFamily(new HColumnDescriptor("e"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");

        if (1 == 1) return;
        ;

        // 取得数据表对象
        Table table = connection.getTable(TableName.valueOf("abc:experiment_51"));

        HColumnDescriptor[] columnFamilies = table.getTableDescriptor().getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            System.out.println(columnFamilies);
        }


        // 取得表中所有数据
        ResultScanner scanner = table.getScanner(new Scan());

        // 循环输出表中的数据
        for (Result result : scanner) {

            System.out.print(Bytes.toString(result.getRow()) + "\t");

            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + "->");
                System.out.print(Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println();
        }

        System.out.println("---------------查询整表数据 END-----------------");
    }
}
