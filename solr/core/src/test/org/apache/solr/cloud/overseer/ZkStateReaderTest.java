/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud.overseer;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.OverseerTest;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.util.TimeOut;

public class ZkStateReaderTest extends SolrTestCaseJ4 {

  private static final long TIMEOUT = 30;

  public void testExternalCollectionWatchedNotWatched() throws Exception{
    Path zkDir = createTempDir("testExternalCollectionWatchedNotWatched");
    ZkTestServer server = new ZkTestServer(zkDir);
    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      // create new collection
      ZkWriteCommand c1 = new ZkWriteCommand("c1",
          new DocCollection("c1", new HashMap<>(), Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME), DocRouter.DEFAULT, 0));
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(c1), null);
      writer.writePendingUpdates();
      reader.forceUpdateCollection("c1");

      assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      reader.registerCore("c1");
      assertFalse(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());
      reader.unregisterCore("c1");
      assertTrue(reader.getClusterState().getCollectionRef("c1").isLazilyLoaded());

    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testCollectionStateWatcherCaching() throws Exception  {
    Path zkDir = createTempDir("testCollectionStateWatcherCaching");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());
      DocCollection state = new DocCollection("c1", new HashMap<>(), Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME), DocRouter.DEFAULT, 0);
      ZkWriteCommand wc = new ZkWriteCommand("c1", state);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
      writer.writePendingUpdates();
      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));
      reader.waitForState("c1", 1, TimeUnit.SECONDS, (liveNodes, collectionState) -> collectionState != null);

      Map<String, Object> props = new HashMap<>();
      props.put("x", "y");
      props.put(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME);
      state = new DocCollection("c1", new HashMap<>(), props, DocRouter.DEFAULT, 0);
      wc = new ZkWriteCommand("c1", state);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
      writer.writePendingUpdates();

      boolean found = false;
      TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      while (!timeOut.hasTimedOut())  {
        DocCollection c1 = reader.getClusterState().getCollection("c1");
        if ("y".equals(c1.getStr("x"))) {
          found = true;
          break;
        }
      }
      assertTrue("Could not find updated property in collection c1 even after 5 seconds", found);
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();
    }
  }

  public void testWatchedCollectionCreation() throws Exception {
    Path zkDir = createTempDir("testWatchedCollectionCreation");

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;

    try {
      server.run();

      zkClient = new SolrZkClient(server.getZkAddress(), OverseerTest.DEFAULT_CONNECTION_TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      reader.registerCore("c1");

      // Initially there should be no c1 collection.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);
      reader.forceUpdateCollection("c1");

      // Still no c1 collection, despite a collection path.
      assertNull(reader.getClusterState().getCollectionRef("c1"));

      ZkStateWriter writer = new ZkStateWriter(reader, new Stats());


      // create new collection
      DocCollection state = new DocCollection("c1", new HashMap<>(), Map.of(ZkStateReader.CONFIGNAME_PROP, ConfigSetsHandler.DEFAULT_CONFIGSET_NAME), DocRouter.DEFAULT, 0);
      ZkWriteCommand wc = new ZkWriteCommand("c1", state);
      writer.enqueueUpdate(reader.getClusterState(), Collections.singletonList(wc), null);
      writer.writePendingUpdates();

      assertTrue(zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", true));

      //reader.forceUpdateCollection("c1");
      reader.waitForState("c1", TIMEOUT, TimeUnit.SECONDS, (n, c) -> c != null);
      ClusterState.CollectionRef ref = reader.getClusterState().getCollectionRef("c1");
      assertNotNull(ref);
      assertFalse(ref.isLazilyLoaded());
    } finally {
      IOUtils.close(reader, zkClient);
      server.shutdown();

    }
  }
}
