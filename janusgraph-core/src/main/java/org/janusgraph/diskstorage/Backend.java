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

package org.janusgraph.diskstorage;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.BaseConfiguration;
import org.janusgraph.core.JanusGraphConfigurationException;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.configuration.*;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.configuration.backend.builder.KCVSConfigurationBuilder;
import org.janusgraph.diskstorage.idmanagement.ConsistentKeyIDAuthority;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.keycolumnvalue.cache.CacheTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.cache.ExpirationKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.cache.NoKCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.*;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerProvider;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLocker;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.LogManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.util.MetricInstrumentedStoreManager;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Orchestrates and configures all backend systems:
 * The primary backend storage ({@link KeyColumnValueStore}) and all external indexing providers ({@link IndexProvider}).
 * 这是用来协调和配置后端的存储系统用的，getStorageManager就会根据配置文件初始化一个存储管理器。 存储管理器，提供KeyColumnValueStore的获取。 用于操作后盾查询，操作等。
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class Backend implements LockerProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Backend.class);

    /**
     * These are the names for the edge store and property index databases, respectively.
     * The edge store contains all edges and properties. The property index contains an
     * inverted index from attribute value to vertex.
     * <p>
     * These names are fixed and should NEVER be changed. Changing these strings can
     * disrupt storage adapters that rely on these names for specific configurations.
     * In the past, the store name for the ID table, janusgraph_ids, was also marked here,
     * but to clear the upgrade path from Titan to JanusGraph, we had to pull it into
     * configuration.
     */
    public static final String EDGESTORE_NAME = "edgestore";
    public static final String INDEXSTORE_NAME = "graphindex";

    public static final String METRICS_STOREMANAGER_NAME = "storeManager";
    public static final String METRICS_MERGED_STORE = "stores";
    public static final String METRICS_MERGED_CACHE = "caches";
    public static final String METRICS_CACHE_SUFFIX = ".cache";
    public static final String LOCK_STORE_SUFFIX = "_lock_";

    public static final String SYSTEM_TX_LOG_NAME = "txlog";
    public static final String SYSTEM_MGMT_LOG_NAME = "systemlog";

    public static final double EDGESTORE_CACHE_PERCENT = 0.8;
    public static final double INDEXSTORE_CACHE_PERCENT = 0.2;

    private static final long ETERNAL_CACHE_EXPIRATION = 1000L *3600*24*365*200; //200 years

    public static final int THREAD_POOL_SIZE_SCALE_FACTOR = 2;

    private final KeyColumnValueStoreManager storeManager; //对一个的是storagManager的实例，可以用来打开数据库底层存储库。
    private final KeyColumnValueStoreManager storeManagerLocking;// 如果支持锁，这个是包装了的storeManager，调用openStore时，将会打开一个本身store也会打开一个store_locak的库。重写了openStore（）方法。
    private final StoreFeatures storeFeatures; //存储管理器的存储特性支持

    private KCVSCache edgeStore;
    private KCVSCache indexStore;
    private KCVSCache txLogStore;
    private IDAuthority idAuthority;
    private KCVSConfiguration systemConfig;//system_store
    private KCVSConfiguration userConfig; //配置类，store是 system_store
    private boolean hasAttemptedClose;

    private final StandardScanner scanner;

    private final KCVSLogManager managementLogManager; //名字叫janusgraph
    private final KCVSLogManager txLogManager; //名字是 tx
    private final LogManager userLogManager;


    private final Map<String, IndexProvider> indexes; //应该是存放第三方索引的

    private final int bufferSize;
    private final Duration maxWriteTime;
    private final Duration maxReadTime;
    private final boolean cacheEnabled;
    private final ExecutorService threadPool;

    private final Function<String, Locker> lockerCreator; //锁处理的创建者。三种方式，默认的一直锁。CONSISTENT_KEY_LOCKER_CREATOR
    private final ConcurrentHashMap<String, Locker> lockers = new ConcurrentHashMap<>();//所有的锁store.

    private final Configuration configuration;
    //这里Backend实例，实例化后有用的是获取到了存储管理器(getStorageManager()的逻辑),三个 KCVSLogManager分别是janusgraph tx 与 user，这是对store/CF的包装KVCSLog的管理类。 初始化了一个并发锁类，
    public Backend(Configuration configuration) {
        this.configuration = configuration;

        //manager持有所有的配置，并且能打开store/CF.
        KeyColumnValueStoreManager manager = getStorageManager(configuration);
        if (configuration.get(BASIC_METRICS)) { //带监控storagemanager的包装类。
            storeManager = new MetricInstrumentedStoreManager(manager,METRICS_STOREMANAGER_NAME,configuration.get(METRICS_MERGE_STORES),METRICS_MERGED_STORE);
        } else {
            storeManager = manager;
        }
        indexes = getIndexes(configuration);
        storeFeatures = storeManager.getFeatures();

        //这里用来获取到了一个KCVSLogManager ，这个是专门用来操作keyColumnStor的日志管理器.一个日志管理器，代表着一个队store/CF的操作类。
        managementLogManager = getKCVSLogManager(MANAGEMENT_LOG); //kvcsLog名为janusgraph的日志管理器。
        txLogManager = getKCVSLogManager(TRANSACTION_LOG);//打开名字为tx的日志管理器。
        userLogManager = getLogManager(USER_LOG); //名字是 user

        //db 缓存开启条件，batch关闭，并且db.cache 开启。
        cacheEnabled = !configuration.get(STORAGE_BATCH) && configuration.get(DB_CACHE);

        int bufferSizeTmp = configuration.get(BUFFER_SIZE);
        Preconditions.checkArgument(bufferSizeTmp > 0, "Buffer size must be positive");
        if (!storeFeatures.hasBatchMutation()) {
            bufferSize = Integer.MAX_VALUE;
        } else bufferSize = bufferSizeTmp;

        maxWriteTime = configuration.get(STORAGE_WRITE_WAITTIME);
        maxReadTime = configuration.get(STORAGE_READ_WAITTIME);

        if (!storeFeatures.hasLocking()) {//
            Preconditions.checkArgument(storeFeatures.isKeyConsistent(),"Store needs to support some form of locking");
            storeManagerLocking = new ExpectedValueCheckingStoreManager(storeManager,LOCK_STORE_SUFFIX,this,maxReadTime);
        } else {
            storeManagerLocking = storeManager;
        }
        //如果启用，JanusGraph 会尝试使用在整个 JanusGraph 图形数据库实例中共享的固定线程池针对存储后端并行化存储操作,并行化仅适用于某些存储操作，并且在操作受 IO 限制时可能是有益的。
        if (configuration.get(PARALLEL_BACKEND_OPS)) {//是否应该并行操作后台管理器，
            int poolSize = Runtime.getRuntime().availableProcessors() * THREAD_POOL_SIZE_SCALE_FACTOR;
            threadPool = Executors.newFixedThreadPool(poolSize);
            log.info("Initiated backend operations thread pool of size {}", poolSize);
        } else {
            threadPool = null;
        }

        final String lockBackendName = configuration.get(LOCK_BACKEND);
        if (REGISTERED_LOCKERS.containsKey(lockBackendName)) {
            lockerCreator = REGISTERED_LOCKERS.get(lockBackendName);
        } else {
            throw new JanusGraphConfigurationException("Unknown lock backend \"" +
                    lockBackendName + "\".  Known lock backends: " +
                    Joiner.on(", ").join(REGISTERED_LOCKERS.keySet()) + ".");
        }
        // Never used for backends that have innate transaction support, but we
        // want to maintain the non-null invariant regardless; it will default
        // to consistent-key implementation if none is specified
        Preconditions.checkNotNull(lockerCreator);
        //这个不知道。
        scanner = new StandardScanner(storeManager);
    }


    @Override
    public Locker getLocker(String lockerName) {

        Preconditions.checkNotNull(lockerName);

        Locker l = lockers.get(lockerName);

        if (null == l) {
            l = lockerCreator.apply(lockerName);
            final Locker x = lockers.putIfAbsent(lockerName, l);
            if (null != x) {
                l = x;
            }
        }

        return l;
    }


    /**
     * Initializes this backend with the given configuration. Must be called before this Backend can be used
     *
     * @param config
     */
    public void initialize(Configuration config) {
        try {
            //EdgeStore & VertexIndexStore 打开了两个新的store，一个是存边的，一个是存点索引的。
            // store的name是 janusgraph_ids 存储的是每个parttion用的id最大值。
            KeyColumnValueStore idStore = storeManager.openDatabase(config.get(IDS_STORE_NAME));

            idAuthority = null;
            if (storeFeatures.isKeyConsistent()) {
                idAuthority = new ConsistentKeyIDAuthority(idStore, storeManager, config);
            } else {
                throw new IllegalStateException("Store needs to support consistent key or transactional operations for ID manager to guarantee proper id allocations");
            }
            //如果支持锁，用锁store包装一层。
            KeyColumnValueStore edgeStoreRaw = storeManagerLocking.openDatabase(EDGESTORE_NAME);
            KeyColumnValueStore indexStoreRaw = storeManagerLocking.openDatabase(INDEXSTORE_NAME);

            //Configure caches  配置缓存用的。 是有对store进行了一次cache包装，在查询store之前，先查询缓存。 在包装一层。
            if (cacheEnabled) {
                long expirationTime = configuration.get(DB_CACHE_TIME);
                Preconditions.checkArgument(expirationTime>=0,"Invalid cache expiration time: %s",expirationTime);
                if (expirationTime==0) expirationTime=ETERNAL_CACHE_EXPIRATION;

                long cacheSizeBytes;
                double cacheSize = configuration.get(DB_CACHE_SIZE);
                Preconditions.checkArgument(cacheSize>0.0,"Invalid cache size specified: %s",cacheSize);
                if (cacheSize<1.0) {
                    //Its a percentage
                    Runtime runtime = Runtime.getRuntime();
                    cacheSizeBytes = (long)((runtime.maxMemory()-(runtime.totalMemory()-runtime.freeMemory())) * cacheSize);
                } else {
                    Preconditions.checkArgument(cacheSize>1000,"Cache size is too small: %s",cacheSize);
                    cacheSizeBytes = (long)cacheSize;
                }
                log.info("Configuring total store cache size: {}",cacheSizeBytes);
                long cleanWaitTime = configuration.get(DB_CACHE_CLEAN_WAIT);
                Preconditions.checkArgument(EDGESTORE_CACHE_PERCENT + INDEXSTORE_CACHE_PERCENT == 1.0,"Cache percentages don't add up!");
                long edgeStoreCacheSize = Math.round(cacheSizeBytes * EDGESTORE_CACHE_PERCENT);
                long indexStoreCacheSize = Math.round(cacheSizeBytes * INDEXSTORE_CACHE_PERCENT);

                edgeStore = new ExpirationKCVSCache(edgeStoreRaw,getMetricsCacheName(EDGESTORE_NAME),expirationTime,cleanWaitTime,edgeStoreCacheSize);
                indexStore = new ExpirationKCVSCache(indexStoreRaw,getMetricsCacheName(INDEXSTORE_NAME),expirationTime,cleanWaitTime,indexStoreCacheSize);
            } else {
                edgeStore = new NoKCVSCache(edgeStoreRaw);
                indexStore = new NoKCVSCache(indexStoreRaw);
            }

            //Just open them so that they are cached 打开了两个新的store。
            txLogManager.openLog(SYSTEM_TX_LOG_NAME);
            managementLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
            txLogStore = new NoKCVSCache(storeManager.openDatabase(SYSTEM_TX_LOG_NAME));


            //Open global configuration 打开全局的配置类。
            KeyColumnValueStore systemConfigStore = storeManagerLocking.openDatabase(SYSTEM_PROPERTIES_STORE_NAME);
            KCVSConfigurationBuilder kcvsConfigurationBuilder = new KCVSConfigurationBuilder();
            systemConfig = kcvsConfigurationBuilder.buildGlobalConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(
                            configuration.get(TIMESTAMP_PROVIDER),
                            storeFeatures.getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            },systemConfigStore,configuration);
            userConfig = kcvsConfigurationBuilder.buildConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return storeManagerLocking.beginTransaction(StandardBaseTransactionConfig.of(configuration.get(TIMESTAMP_PROVIDER)));
                }

                @Override
                public void close() throws BackendException {
                    //Do nothing, storeManager is closed explicitly by Backend
                }
            },systemConfigStore,USER_CONFIGURATION_IDENTIFIER,configuration);

        } catch (BackendException e) {
            throw new JanusGraphException("Could not initialize backend", e);
        }
    }

    /**
     * Get information about all registered {@link IndexProvider}s.
     *
     * @return
     */
    public Map<String, IndexInformation> getIndexInformation() {
        ImmutableMap.Builder<String, IndexInformation> copy = ImmutableMap.builder();
        copy.putAll(indexes);
        return copy.build();
    }
//
//    public IndexProvider getIndexProvider(String name) {
//        return indexes.get(name);
//    }

    public KCVSLog getSystemTxLog() {
        try {
            return txLogManager.openLog(SYSTEM_TX_LOG_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not re-open transaction log", e);
        }
    }

    public Log getSystemMgmtLog() {
        try {
            return managementLogManager.openLog(SYSTEM_MGMT_LOG_NAME);
        } catch (BackendException e) {
            throw new JanusGraphException("Could not re-open management log", e);
        }
    }

    public StandardScanner.Builder buildEdgeScanJob() {
        return buildStoreIndexScanJob(EDGESTORE_NAME);
    }

    public StandardScanner.Builder buildGraphIndexScanJob() {
        return buildStoreIndexScanJob(INDEXSTORE_NAME);
    }

    private StandardScanner.Builder buildStoreIndexScanJob(String storeName) {
        TimestampProvider provider = configuration.get(TIMESTAMP_PROVIDER);
        ModifiableConfiguration jobConfig = buildJobConfiguration();
        jobConfig.set(JOB_START_TIME,provider.getTime().toEpochMilli());
        return scanner.build()
                .setStoreName(storeName)
                .setTimestampProvider(provider)
                .setJobConfiguration(jobConfig)
                .setGraphConfiguration(configuration)
                .setNumProcessingThreads(1)
                .setWorkBlockSize(10000);
    }

    public JanusGraphManagement.IndexJobFuture getScanJobStatus(Object jobId) {
        return scanner.getRunningJob(jobId);
    }

    public Log getUserLog(String identifier) throws BackendException {
        return userLogManager.openLog(getUserLogName(identifier));
    }

    public static String getUserLogName(String identifier) {
        Preconditions.checkArgument(StringUtils.isNotBlank(identifier));
        return USER_LOG_PREFIX +identifier;
    }

    public KCVSConfiguration getGlobalSystemConfig() {
        return systemConfig;
    }

    public KCVSConfiguration getUserConfiguration() {
        return userConfig;
    }

    private String getMetricsCacheName(String storeName) {
        if (!configuration.get(BASIC_METRICS)) return null;
        return configuration.get(METRICS_MERGE_STORES) ? METRICS_MERGED_CACHE : storeName + METRICS_CACHE_SUFFIX;
    }

    //打开一个日志管理器，传入日志管理器的名字，返回一个实例，
    public KCVSLogManager getKCVSLogManager(String logName) {
        Preconditions.checkArgument(configuration.restrictTo(logName).get(LOG_BACKEND).equalsIgnoreCase(LOG_BACKEND.getDefaultValue()));
        return (KCVSLogManager)getLogManager(logName);
    }

    public LogManager getLogManager(String logName) {
        return getLogManager(configuration, logName, storeManager);
    }
    //创建一个LogManager，只有 KCVSLogManager 针对KeyColumnValueStoreManager的实现。
    private static LogManager getLogManager(Configuration config, String logName, KeyColumnValueStoreManager sm) {
        Configuration logConfig = config.restrictTo(logName);
        String backend = logConfig.get(LOG_BACKEND);//default
        if (backend.equalsIgnoreCase(LOG_BACKEND.getDefaultValue())) {
            return new KCVSLogManager(sm,logConfig);
        } else {
            Preconditions.checkArgument(config!=null);
            LogManager lm = getImplementationClass(logConfig,logConfig.get(LOG_BACKEND),REGISTERED_LOG_MANAGERS);
            Preconditions.checkNotNull(lm);
            return lm;
        }

    }

    /**
     *根据配置主要是storage.backend,从这个类里面获取到org.janusgraph.diskstorage.StandardStoreManager，
     * 这个存储对应的StoreManager的实现类。
     * 这个将会触发Storage的初始化操作，对于Hbase的管理器，会获取到hbase的connection。
     *{@code HBaseStoreManager}
     * @param storageConfig
     * @return
     */
    public static KeyColumnValueStoreManager getStorageManager(Configuration storageConfig) {
        //根据storage.bakend 获取存储管理器，这里面获取到了hbase的连接了。放在了HbaseStoreManager的ConnectionMask cnx属性中。
        StoreManager manager = getImplementationClass(storageConfig, storageConfig.get(STORAGE_BACKEND),
                StandardStoreManager.getAllManagerClasses());
        if (manager instanceof OrderedKeyValueStoreManager) {
            manager = new OrderedKeyValueStoreManagerAdapter((OrderedKeyValueStoreManager) manager,
                ImmutableMap.of(EDGESTORE_NAME, 8, EDGESTORE_NAME + LOCK_STORE_SUFFIX, 8,
                    storageConfig.get(IDS_STORE_NAME), 8));
        }
        Preconditions.checkArgument(manager instanceof KeyColumnValueStoreManager,"Invalid storage manager: %s",manager.getClass());
        return (KeyColumnValueStoreManager) manager;
    }

    private static Map<String, IndexProvider> getIndexes(Configuration config) {
        ImmutableMap.Builder<String, IndexProvider> builder = ImmutableMap.builder();
        for (String index : config.getContainedNamespaces(INDEX_NS)) {
            Preconditions.checkArgument(StringUtils.isNotBlank(index), "Invalid index name [%s]", index);
            log.info("Configuring index [{}]", index);
            IndexProvider provider = getImplementationClass(config.restrictTo(index), config.get(INDEX_BACKEND,index),
                    StandardIndexProvider.getAllProviderClasses());
            Preconditions.checkNotNull(provider);
            builder.put(index, provider);
        }
        return builder.build();
    }

    public static <T> T getImplementationClass(Configuration config, String className, Map<String, String> registeredImplementations) {
        if (registeredImplementations.containsKey(className.toLowerCase())) {
            className = registeredImplementations.get(className.toLowerCase());
        }

        return ConfigurationUtil.instantiate(className, new Object[]{config}, new Class[]{Configuration.class});
    }


    /**
     * Returns the configured {@link IDAuthority}.
     *
     * @return
     */
    public IDAuthority getIDAuthority() {
        return Preconditions.checkNotNull(idAuthority, "Backend has not yet been initialized");
    }

    /**
     * Returns the {@link StoreFeatures} of the configured backend storage engine.
     *
     * @return
     */
    public StoreFeatures getStoreFeatures() {
        return storeFeatures;
    }

    public Class<? extends KeyColumnValueStoreManager> getStoreManagerClass() {
        return storeManager.getClass();
    }

    public StoreManager getStoreManager() {
        return storeManager;
    }

    /**
     * Returns the {@link IndexFeatures} of all configured index backends
     */
    public Map<String,IndexFeatures> getIndexFeatures() {
        return Maps.transformValues(indexes,new Function<IndexProvider, IndexFeatures>() {
            @Nullable
            @Override
            public IndexFeatures apply(@Nullable IndexProvider indexProvider) {
                return indexProvider.getFeatures();
            }
        });
    }

    /**
     * Opens a new transaction against all registered backend system wrapped in one {@link BackendTransaction}.
     *
     * @return
     * @throws BackendException
     */
    public BackendTransaction beginTransaction(TransactionConfiguration configuration, KeyInformation.Retriever indexKeyRetriever) throws BackendException {

        StoreTransaction tx = storeManagerLocking.beginTransaction(configuration);

        // Cache
        CacheTransaction cacheTx = new CacheTransaction(tx, storeManagerLocking, bufferSize, maxWriteTime, configuration.hasEnabledBatchLoading());

        // Index transactions
        final Map<String, IndexTransaction> indexTx = new HashMap<>(indexes.size());
        for (Map.Entry<String, IndexProvider> entry : indexes.entrySet()) {
            indexTx.put(entry.getKey(), new IndexTransaction(entry.getValue(), indexKeyRetriever.get(entry.getKey()), configuration, maxWriteTime));
        }

        return new BackendTransaction(cacheTx, configuration, storeFeatures,
                edgeStore, indexStore, txLogStore,
                maxReadTime, indexTx, threadPool);
    }

    public synchronized void close() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            managementLogManager.close();
            log.info("Closing "+ this.getStoreManager().getName());
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            if (edgeStore != null) edgeStore.close();
            if (indexStore != null) indexStore.close();
            if (idAuthority != null) idAuthority.close();
            if (systemConfig != null) systemConfig.close();
            if (userConfig != null) userConfig.close();
            storeManager.close();
            if(threadPool != null) {
            	threadPool.shutdown();
            }
            //Indexes
            for (IndexProvider index : indexes.values()) index.close();
        } else {
            log.debug("Backend {} has already been closed or cleared", this);
        }
    }

    /**
     * Clears the storage of all registered backend data providers. This includes backend storage engines and index providers.
     * <p>
     * IMPORTANT: Clearing storage means that ALL data will be lost and cannot be recovered.
     *
     * @throws BackendException
     */
    public synchronized void clearStorage() throws BackendException {
        if (!hasAttemptedClose) {
            hasAttemptedClose = true;
            managementLogManager.close();
            txLogManager.close();
            userLogManager.close();

            scanner.close();
            edgeStore.close();
            indexStore.close();
            idAuthority.close();
            systemConfig.close();
            userConfig.close();
            storeManager.clearStorage();
            storeManager.close();
            //Indexes
            for (IndexProvider index : indexes.values()) {
                index.clearStorage();
                index.close();
            }
        } else {
            log.debug("Backend {} has already been closed or cleared", this);
        }
    }

    private ModifiableConfiguration buildJobConfiguration() {

        return new ModifiableConfiguration(JOB_NS,
            new CommonsConfiguration(new BaseConfiguration()),
            BasicConfiguration.Restriction.NONE);
    }

    //############ Registered Storage Managers ##############

    private static final ImmutableMap<StandardStoreManager, ConfigOption<?>> STORE_SHORTHAND_OPTIONS;

    static {
        final Map<StandardStoreManager, ConfigOption<?>> m = new HashMap<>();

        m.put(StandardStoreManager.BDB_JE, STORAGE_DIRECTORY);
        m.put(StandardStoreManager.CASSANDRA_ASTYANAX, STORAGE_HOSTS);
        m.put(StandardStoreManager.CASSANDRA_EMBEDDED, STORAGE_CONF_FILE);
        m.put(StandardStoreManager.CASSANDRA_THRIFT, STORAGE_HOSTS);
        m.put(StandardStoreManager.HBASE, STORAGE_HOSTS);
        //m.put(StandardStorageBackend.IN_MEMORY, null);

        //STORE_SHORTHAND_OPTIONS = Maps.immutableEnumMap(m);
        STORE_SHORTHAND_OPTIONS = ImmutableMap.copyOf(m);
    }

    public static ConfigOption<?> getOptionForShorthand(String shorthand) {
        if (null == shorthand)
            return null;

        shorthand = shorthand.toLowerCase();

        for (StandardStoreManager m : STORE_SHORTHAND_OPTIONS.keySet()) {
            if (m.getShorthands().contains(shorthand))
                return STORE_SHORTHAND_OPTIONS.get(m);
        }

        return null;
    }

    public static final Map<String,String> REGISTERED_LOG_MANAGERS = new HashMap<String, String>() {{
        put("default","org.janusgraph.diskstorage.log.kcvs.KCVSLogManager");
    }};

    private final Function<String, Locker> CONSISTENT_KEY_LOCKER_CREATOR = new Function<String, Locker>() {
        @Override
        public Locker apply(String lockerName) {
            KeyColumnValueStore lockerStore;
            try {
                lockerStore = storeManager.openDatabase(lockerName);//打开了一个store，是以storename+_lock_命名的。
            } catch (BackendException e) {
                throw new JanusGraphConfigurationException("Could not retrieve store named " + lockerName + " for locker configuration", e);
            }
            return new ConsistentKeyLocker.Builder(lockerStore, storeManager).fromConfig(configuration).build();
        }
    };

    private final Function<String, Locker> ASTYANAX_RECIPE_LOCKER_CREATOR = new Function<String, Locker>() {

        @Override
        public Locker apply(String lockerName) {

            String expectedManagerName = "org.janusgraph.diskstorage.cassandra.astyanax.AstyanaxStoreManager";
            String actualManagerName = storeManager.getClass().getCanonicalName();
            // Require AstyanaxStoreManager
            Preconditions.checkArgument(expectedManagerName.equals(actualManagerName),
                    "Astyanax Recipe locker is only supported with the Astyanax storage backend (configured:"
                            + actualManagerName + " != required:" + expectedManagerName + ")");

            try {
                Class<?> c = storeManager.getClass();
                Method method = c.getMethod("openLocker", String.class);
                Object o = method.invoke(storeManager, lockerName);
                return (Locker) o;
            } catch (NoSuchMethodException e) {
                throw new IllegalArgumentException("Could not find method when configuring locking with Astyanax Recipes");
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Could not access method when configuring locking with Astyanax Recipes", e);
            } catch (InvocationTargetException e) {
                throw new IllegalArgumentException("Could not invoke method when configuring locking with Astyanax Recipes", e);
            }
        }
    };

    private static final Function<String, Locker> TEST_LOCKER_CREATOR = lockerName -> openManagedLocker("org.janusgraph.diskstorage.util.TestLockerManager",lockerName);

    private final Map<String, Function<String, Locker>> REGISTERED_LOCKERS = ImmutableMap.of(
            "consistentkey", CONSISTENT_KEY_LOCKER_CREATOR,
            "astyanaxrecipe", ASTYANAX_RECIPE_LOCKER_CREATOR,
            "test", TEST_LOCKER_CREATOR
    );

    private static Locker openManagedLocker(String classname, String lockerName) {
        try {
            Class c = Class.forName(classname);
            Constructor constructor = c.getConstructor();
            Object instance = constructor.newInstance();
            Method method = c.getMethod("openLocker", String.class);
            Object o = method.invoke(instance, lockerName);
            return (Locker) o;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find implementation class: " + classname);
        } catch (InstantiationException | ClassCastException e) {
            throw new IllegalArgumentException("Could not instantiate implementation: " + classname, e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find method when configuring locking for: " + classname,e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Could not access method when configuring locking for: " + classname,e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Could not invoke method when configuring locking for: " + classname,e);
        }
    }
}
