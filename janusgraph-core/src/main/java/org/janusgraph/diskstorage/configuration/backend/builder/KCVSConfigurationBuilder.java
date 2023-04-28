// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration.backend.builder;

import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**这是用来创建KCVSConfiguration，不知道这个是干涉用的？？
 * Builder to build {@link KCVSConfiguration} instances
 */
public class KCVSConfigurationBuilder {

    /**
     * 用来 创建KCVSConfiguration，这是个配置类，里面有个KeyColumnValueStore对象，可以用来访问存储层的一个数据库，对应hbase的
     * 是一个列簇。 KeyColumnValueStore对象的创建是在
     * {@code manager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME)}system_properties，会在hbase中创建一个列簇，这个也会store也会
     * 被缓存到StorageManager中。
     *
     * @param manager
     * @param config
     * @return
     */
    public KCVSConfiguration buildStandaloneGlobalConfiguration(final KeyColumnValueStoreManager manager, final Configuration config) {
        try {
            final StoreFeatures features = manager.getFeatures();
            return buildGlobalConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return manager.beginTransaction(StandardBaseTransactionConfig.of(config.get(TIMESTAMP_PROVIDER),features.getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    manager.close();
                }
            },manager.openDatabase(SYSTEM_PROPERTIES_STORE_NAME),config);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open global configuration",e);
        }
    }

    public KCVSConfiguration buildConfiguration(final BackendOperation.TransactionalProvider txProvider,
                                                final KeyColumnValueStore store, final String identifier,
                                                final Configuration config) {
        try {
            KCVSConfiguration keyColumnValueStoreConfiguration =
                new KCVSConfiguration(txProvider,config,store,identifier);
            keyColumnValueStoreConfiguration.setMaxOperationWaitTime(config.get(SETUP_WAITTIME));
            return keyColumnValueStoreConfiguration;
        } catch (BackendException e) {
            throw new JanusGraphException("Could not open global configuration",e);
        }
    }

    /**
     * 创建配置，
     * @param txProvider 用来获取事务StoreTransaction对象的工厂方法。
     * @param store
     * @param config 配置文件信息
     * @return
     */
    public KCVSConfiguration buildGlobalConfiguration(final BackendOperation.TransactionalProvider txProvider,
                                                      final KeyColumnValueStore store,
                                                      final Configuration config) {
        return buildConfiguration(txProvider, store, SYSTEM_CONFIGURATION_IDENTIFIER, config);
    }
}
