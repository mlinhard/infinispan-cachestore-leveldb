package org.infinispan.loaders.leveldb;

import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.loaders.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.leveldb.JavaLevelDBCacheStoreFunctionalTest")
public class JavaLevelDBCacheStoreFunctionalTest extends LevelDBCacheStoreFunctionalTest {
   public static final int EXPIRATION_TIMEOUT = 3000;
   public static final int EVICTION_CHECK_TIMEOUT = 2000;

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      super.createStoreBuilder(loaders).implementationType(LevelDBStoreConfiguration.ImplementationType.JAVA).preload(preload);
      return loaders;
   }

   public void testEntrySetAfterExpiryWithStore(Method m) throws Exception {
      CacheContainer cc = createCacheContainerWithStore();
      try {
         Cache<Integer, String> cache = cc.getCache();
         Set<Map.Entry<Integer, String>> entries;
         Map dataIn = new HashMap();
         dataIn.put(1, v(m, 1));
         dataIn.put(2, v(m, 2));
         Set entriesIn = dataIn.entrySet();

         cache.putAll(dataIn, EXPIRATION_TIMEOUT, TimeUnit.MILLISECONDS);

         Thread.sleep(EXPIRATION_TIMEOUT + 1000);
         entries = cache.entrySet();
         assert entries.size() == 0;
      } finally {
         cc.stop();
      }
   }

   private CacheContainer createCacheContainerWithStore() {
      ConfigurationBuilder b = new ConfigurationBuilder();
      createCacheStoreConfig(b.persistence(), false);
      return TestCacheManagerFactory.createCacheManager(b);
   }

}
