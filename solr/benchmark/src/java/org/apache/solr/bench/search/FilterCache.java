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
package org.apache.solr.bench.search;

import static org.apache.solr.bench.generators.SourceDSL.integers;

import java.io.IOException;
import org.apache.solr.bench.Docs;
import org.apache.solr.bench.MiniClusterState;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.Constraint;

@Fork(value = 1)
public class FilterCache {

  static final String COLLECTION = "c1";

  @State(Scope.Benchmark)
  public static class BenchState {
    @Param({"true", "false"})
    String asyncCache;

    @Param({"2", "50", "98"}) // rarely, sometimes, usually
    String frequency;

    QueryRequest q1 = new QueryRequest(new SolrQuery("q", "*:*", "fq", "Ea_b:true"));
    QueryRequest q2 = new QueryRequest(new SolrQuery("q", "*:*", "fq", "FB_b:true"));

    @Setup(Level.Trial)
    public void setupTrial(MiniClusterState.MiniClusterBenchState miniClusterState) throws Exception {
      System.setProperty("filterCache.enabled", "true");
      System.setProperty("filterCache.async", asyncCache);

      miniClusterState.startMiniCluster(1);
      miniClusterState.createCollection(COLLECTION, 1, 1);

      Docs docs = Docs.docs().field("id", integers().incrementing());

      SolrGen<Boolean> booleans = new SolrGen<>() {
        int threshold = Integer.parseInt(frequency);

        @Override
        public Boolean generate(RandomnessSource in) {
          return in.next(Constraint.between(0, 100)) < threshold;
        }
      };
      // Field names chosen purposely to create hash collision
      docs.field("Ea_b", booleans);
      docs.field("FB_b", booleans);

      miniClusterState.index(COLLECTION, docs, 100 * 1000);
      q1.setBasePath(miniClusterState.nodes.get(0));
      q2.setBasePath(miniClusterState.nodes.get(0));
    }

    @Setup(Level.Iteration)
    public void setupIteration(MiniClusterState.MiniClusterBenchState miniClusterState)
        throws SolrServerException, IOException {
      // Reload the collection/core to drop existing caches
      CollectionAdminRequest.Reload reload = CollectionAdminRequest.reloadCollection(COLLECTION);
      reload.setBasePath(miniClusterState.nodes.get(0));
      miniClusterState.client.request(reload);
    }
  }

  @Benchmark
  @Warmup(time = 2, iterations = 10)
  @Measurement(time = 2, iterations = 10)
  @Threads(value = Threads.MAX)
  public Object jsonFacet(
      BenchState benchState, MiniClusterState.MiniClusterBenchState miniClusterState)
      throws SolrServerException, IOException {
    return miniClusterState.client.request(miniClusterState.getRandom().nextBoolean() ? benchState.q1 : benchState.q2, COLLECTION);
  }
}
