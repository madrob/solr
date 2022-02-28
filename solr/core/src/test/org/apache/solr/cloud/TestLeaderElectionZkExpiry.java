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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.common.cloud.ConnectionManager=ERROR;org.apache.solr.cloud.Overseer=ERROR;org.apache.solr.common.cloud.SolrZkClient=DEBUG")
public class TestLeaderElectionZkExpiry extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SOLRXML = "<solr></solr>";
  private static final int MAX_NODES = 16;
  private static final int MIN_NODES = 4;

  private CountDownLatch foundLeader;

  @Test
  public void testLeaderElectionWithZkExpiry() throws Exception {
    Path zkDir = createTempDir("zkData");
    Path ccDir = createTempDir("testLeaderElectionWithZkExpiry-solr");
    CoreContainer cc = createCoreContainer(ccDir, SOLRXML);
    final ZkTestServer server = new ZkTestServer(zkDir);
    server.setTheTickTime(1000);
    try {
      server.run();

      CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("dummy.host.com", 8984, "solr")
          .setLeaderConflictResolveWait(180000)
          .setLeaderVoteWait(180000)
          .build();

      try (ZkController zkController = new ZkController(cc, server.getZkAddress(), 15000, cloudConfig, Collections::emptyList)) {
        ScheduledExecutorService work = Executors.newSingleThreadScheduledExecutor(new SolrNamedThreadFactory("work"));
        // Expire session every 10ms
        ScheduledFuture<?> expireTask = work.scheduleWithFixedDelay(() -> server.expire(zkController.getZkClient().getSolrZooKeeper().getSessionId()), 0, 10, TimeUnit.MILLISECONDS);
        // Stop exirations after 10s, for approximately 1000 total expirations
        ScheduledFuture<Boolean> done = work.schedule(() -> expireTask.cancel(true), 5, TimeUnit.SECONDS);
        assertTrue("Cancelled expiration tasks.", done.get());
        work.shutdown(); // shouldn't need additional wait because both tasks have completed
        log.info("Finished with session expiration phase of the test.");

        // ZkContoller maintains the underlying election, so looking for a leader must happen while it is still open
        try (SolrZkClient zc = new SolrZkClient(server.getZkAddress(), LeaderElectionTest.TIMEOUT)) {
          foundLeader = new CountDownLatch(1);
          Thread waitForLeader = new Thread(() -> {
            Watcher watcher = new Watcher() {
              @Override
              public void process(WatchedEvent event) {
                checkLeader(zc, this);
              }
            };
            checkLeader(zc, watcher);
          }, "waitForLeader");
          waitForLeader.start();

          assertTrue("Somebody should have joined the list of electors.", foundLeader.await(5, TimeUnit.SECONDS));

          // TODO: rewrite this to use ZK watch instead of polling
          TimeOut timeout = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
          String leaderNode = null;
          long retries = 0;
          while (timeout.hasTimedOut() == false) {
            leaderNode = OverseerCollectionConfigSetProcessor.getLeaderNode(zc);
            if (leaderNode == null) {
              retries++;
            }
          }
          log.info("retries: {}", retries);
          assertNotNull(leaderNode);
          assertFalse("leader node should not be empty", leaderNode.trim().isEmpty());
        } // zc
      } // zkController
    } finally {
      cc.shutdown();
      server.shutdown();
    }
  }

  private void checkLeader(SolrZkClient zc, Watcher watcher) {
    final String electionPath = Overseer.OVERSEER_ELECT;
    try {
      Stat stat = zc.exists(electionPath + "/leader", null, true);
      if (stat != null) {
        log.info("Found leader, stat={}", stat);
        foundLeader.countDown();
      } else {
        if (zc.getChildren(electionPath, watcher, true).contains("leader")) {
          log.info("Found leader as child node while setting watch");
          foundLeader.countDown();
        }
      }
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      throw new RuntimeException(e);
    }
  }
}
