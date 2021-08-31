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

import static org.apache.solr.bench.BaseBenchState.log;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.bench.generators.MultiString;
import org.apache.solr.bench.generators.SolrGen;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.BenchmarkRandomSource;

/**
 * A tool to generate controlled random data for a benchmark. {@link SolrInputDocument}s are created
 * based on supplied FieldDef definitions.
 *
 * <p>You can call getDocument to build and retrieve one {@link SolrInputDocument} at a time, or you
 * can call {@link #preGenerate} to generate the given number of documents in RAM, and then retrieve
 * them via {@link #generatedDocsIterator}.
 */
public class Docs {

  private final ThreadLocal<SolrRandomnessSource> random;
  private Queue<SolrInputDocument> docs = new ConcurrentLinkedQueue<>();

  private final Map<String, Gen<?>> fields = new HashMap<>(16);

  private ExecutorService executorService;
  private int stringFields;
  private int multiStringFields;
  private int integerFields;
  private int longFields;
  private int booleanFields;
  private int floatFields;
  private int dateFields;
  private int doubleFields;

  public static Docs docs() {
    return new Docs(BaseBenchState.getRandomSeed());
  }

  public static Docs docs(Long seed) {
    return new Docs(seed);
  }

  private Docs(Long seed) {
    this.random =
        ThreadLocal.withInitial(
            () ->
                new BenchmarkRandomSource(
                    new SplittableRandomGenerator(seed))); // TODO: pluggable RandomGenerator
  }

  @SuppressForbidden(reason = "This module does not need to deal with logging context")
  public Iterator<SolrInputDocument> preGenerate(int numDocs) throws InterruptedException {
    log("preGenerate docs " + numDocs + " ...");
    docs.clear();
    executorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() + 1,
            new SolrNamedThreadFactory("SolrJMH DocMaker"));

    for (int i = 0; i < numDocs; i++) {
      executorService.submit(
          () -> {
            try {
              SolrInputDocument doc = Docs.this.inputDocument();
              docs.add(doc);
            } catch (Exception e) {
              e.printStackTrace();
              executorService.shutdownNow();
              throw new RuntimeException(e);
            }
          });
    }

    executorService.shutdown();
    boolean result = executorService.awaitTermination(10, TimeUnit.MINUTES);
    if (!result) {
      throw new RuntimeException("Timeout waiting for doc adds to finish");
    }
    log(
        "done preGenerateDocs docs="
            + docs.size()
            + " ram="
            + RamUsageEstimator.humanReadableUnits(RamUsageEstimator.sizeOfObject(docs)));

    if (numDocs != docs.size()) {
      throw new IllegalStateException("numDocs != " + docs.size());
    }

    return docs.iterator();
  }

  public Iterator<SolrInputDocument> generatedDocsIterator() {
    return docs.iterator();
  }

  public SolrInputDocument inputDocument() {
    SolrInputDocument doc = new SolrInputDocument();
    SolrRandomnessSource randomSource = random.get();
    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(randomSource));
    }

    return doc;
  }

  public SolrDocument document() {
    SolrDocument doc = new SolrDocument();
    SolrRandomnessSource randomSource = random.get();
    for (Map.Entry<String, Gen<?>> entry : fields.entrySet()) {
      doc.addField(entry.getKey(), entry.getValue().generate(randomSource));
    }

    return doc;
  }

  public Docs field(String name, Gen<?> generator) {
    fields.put(name, generator);
    return this;
  }

  public Docs field(SolrGen<?> generator) {
    Class type = generator.type();
    if (String.class == type) {
      fields.put("string" + (stringFields++ > 0 ? stringFields : "") + "_s", generator);
    } else if (MultiString.class == type) {
      fields.put("text" + (multiStringFields++ > 0 ? multiStringFields : "") + "_t", generator);
    } else if (Integer.class == type) {
      fields.put("int" + (integerFields++ > 0 ? integerFields : "") + "_t", generator);
    } else if (Long.class == type) {
      fields.put("long" + (longFields++ > 0 ? longFields : "") + "_t", generator);
    } else if (Boolean.class == type) {
      fields.put("boolean" + (booleanFields++ > 0 ? booleanFields : "") + "_b", generator);
    } else if (Float.class == type) {
      fields.put("float" + (floatFields++ > 0 ? floatFields : "") + "_f", generator);
    } else if (Date.class == type) {
      fields.put("date" + (dateFields++ > 0 ? dateFields : "") + "_dt", generator);
    } else if (Double.class == type) {
      fields.put("double" + (doubleFields++ > 0 ? doubleFields : "") + "_d", generator);
    } else {
      throw new IllegalArgumentException("Unknown type: " + generator.type());
    }

    return this;
  }

  public void clear() {
    docs.clear();
  }
}
