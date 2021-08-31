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
package org.apache.solr.bench;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.SplittableRandom;
import javax.management.MBeanServer;
import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class BaseBenchState {

  private static final long RANDOM_SEED = 6624420638116043983L;

  private static final SplittableRandom random = new SplittableRandom(getInitRandomeSeed());

  public static Long getRandomSeed() {
    return random.split().nextLong();
  }

  public static final boolean QUIET_LOG = Boolean.getBoolean("quietLog");

  @SuppressForbidden(reason = "JMH uses std out for user output")
  public static void log(String value) {
    if (!QUIET_LOG) {
      System.out.println((value.isEmpty() ? "" : "--> ") + value);
    }
  }

  @Setup(Level.Trial)
  public void doSetup(BenchmarkParams benchmarkParams) {
    System.setProperty("solr.log.name", benchmarkParams.id());
  }

  @TearDown(Level.Trial)
  public static void doTearDown(BenchmarkParams benchmarkParams) throws Exception {
    String heapDump = System.getProperty("dumpheap");
    if (heapDump != null) {
      File file = new File(heapDump);
      FileUtils.deleteDirectory(file);
      file.mkdirs();
      File dumpFile = new File(file, benchmarkParams.id() + ".hprof");

      dumpHeap(dumpFile.getAbsolutePath(), true);
    }
  }

  @SuppressForbidden(reason = "access to force heapdump")
  public static void dumpHeap(String filePath, boolean live) throws IOException {
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    HotSpotDiagnosticMXBean mxBean =
        ManagementFactory.newPlatformMXBeanProxy(
            server, "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
    mxBean.dumpHeap(filePath, live);
  }

  private static Long getInitRandomeSeed() {
    Long seed = Long.getLong("solr.bench.seed");

    if (seed == null) {
      seed = RANDOM_SEED;
    }

    log("benchmark random seed: " + seed);

    return seed;
  }
}
