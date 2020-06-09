package org.janusgraph;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.hbase.HBaseStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Quartet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Think on 2019/4/8.
 */
public class test2 {

    public static void main(String[] args) {
        Map<String, Double> map = new HashMap<>();
        map.put("3",1.0);
        map.put("2",1.0);
        map.put("4",2.0);
        map.put("5",1.0);
        map.put("1",1.0);
        largestCount(map);
    }
    private static <T> Quartet<T,Double,T,Double> largestCount(final Map<T, Double> map) {
        T largestKey = null;
        T secondKey = null;
        double largestValue = Double.MIN_VALUE;
        double secondValue = Double.MIN_VALUE;
        for (Map.Entry<T, Double> entry : map.entrySet()) {
            if(entry.getValue() > largestValue){
                secondKey = largestKey;
                secondValue = largestValue;
                largestKey = entry.getKey();
                largestValue = entry.getValue();
                continue;
            } else if (entry.getValue() == largestValue){
                if (null != largestKey && largestKey.toString().compareTo(entry.getKey().toString()) > 0) {
                    secondKey = largestKey;
                    secondValue = largestValue;
                    largestKey = entry.getKey();
                    largestValue = entry.getValue();
                    continue;
                }
            }
            if (entry.getValue() > secondValue){
                secondKey = entry.getKey();
                secondValue = entry.getValue();
            } else if (entry.getValue() == secondValue){
                if (null != secondKey && secondKey.toString().compareTo(entry.getKey().toString()) > 0){
                    secondKey = entry.getKey();
                    secondValue = entry.getValue();
                }
            }

//            if (entry.getValue() == largestValue) {
//                if (null != largestKey && largestKey.toString().compareTo(entry.getKey().toString()) > 0) {
//                    largestKey = entry.getKey();
//                    largestValue = entry.getValue();
//                }
//            } else if (entry.getValue() > largestValue) {
//                largestKey = entry.getKey();
//                largestValue = entry.getValue();
//            }
        }

        return new Quartet<>(largestKey, largestValue, secondKey, secondValue);
    }
}
