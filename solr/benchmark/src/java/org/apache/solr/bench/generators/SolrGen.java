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
package org.apache.solr.bench.generators;

import java.util.function.Function;
import org.apache.solr.bench.SolrRandomnessSource;
import org.quicktheories.api.AsString;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.BenchmarkRandomSource;

public class SolrGen<T> implements Gen<T> {

  protected final Gen<T> child;
  private final Class<?> type;
  private Distribution distribution = Distribution.UNIFORM;

  protected long start;
  protected long end;
  private RandomDataHistogram.Counts collector;

  protected SolrGen() {
    child = null;
    this.type = null;
  }

  public SolrGen(Gen<T> child, Class<?> type) {
    this.child = child;
    if (child instanceof SolrGen) {
      ((SolrGen<?>) child).distribution = distribution;
    }
    this.type = type;
  }

  SolrGen(Class<?> type) {
    child = null;
    this.type = type;
  }

  protected SolrGen(Gen<T> child) {
    this.child = child;
    this.type = null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T generate(RandomnessSource in) {
    T val;
    if (child == null && start == end) {
      val = generate((SolrRandomnessSource) in);
      if (collector != null) collector.collect(val);
      return val;
    }

    if (child == null) {
      val = (T) Integer.valueOf((int) ((SolrRandomnessSource) in).next(start, end));
      if (collector != null) collector.collect(val);
      return val;
    }

    if (child instanceof SolrGen && in instanceof SolrRandomnessSource) {
      val =
          (T)
              ((SolrGen<Object>) child)
                  .generate(((SolrRandomnessSource) in).withDistribution(distribution));
      if (collector != null) collector.collect(val);
      return val;
    }

    if (in instanceof BenchmarkRandomSource) {
      val = child.generate(((BenchmarkRandomSource) in).withDistribution(distribution));
      if (collector != null) collector.collect(val);
      return val;
    }
    val = child.generate(in);
    if (collector != null) collector.collect(val);
    return val;
  }

  @SuppressWarnings("unchecked")
  public T generate(SolrRandomnessSource in) {
    if (child == null && start == end) {
      return generate((RandomnessSource) in);
    }

    if (child == null) {
      T val = (T) Integer.valueOf((int) in.next(start, end));
      if (collector != null) collector.collect(val);
      return val;
    }

    return child.generate(in);
  }

  @SuppressWarnings("unchecked")
  public Class<?> type() {
    SolrGen<?> other = this;
    while (true) {
      if (other.type == null && other.child instanceof SolrGen) {
        other = ((SolrGen<Object>) other.child);
        continue;
      }
      return other.type;
    }
  }

  @Override
  public SolrGen<T> describedAs(AsString<T> asString) {
    return new SolrDescribingGenerator<>(this, asString);
  }

  public SolrGen<T> mix(Gen<T> rhs, Class<?> type) {
    return mix(rhs, 50, type);
  }

  @Override
  public Gen<T> mix(Gen<T> rhs, int weight) {
    return mix(rhs, weight, null);
  }

  public SolrGen<T> mix(Gen<T> rhs, int weight, Class<?> type) {
    return new SolrGen<>(type) {
      @Override
      public T generate(SolrRandomnessSource in) {
        while (true) {
          long picked = in.next(0, 99);
          if (picked >= weight) {
            continue;
          }
          return ((SolrGen<T>) rhs).generate(in);
        }
      }
    };
  }

  /**
   * Flat maps generated values with supplied function
   *
   * @param mapper function to map with
   * @param <R> Type to map to
   * @return A Gen of R
   */
  @Override
  public <R> Gen<R> flatMap(Function<? super T, Gen<? extends R>> mapper) {
    return new SolrGen<>(in -> mapper.apply(generate(in)).generate(in), null);
  }

  @Override
  public <R> Gen<R> map(Function<? super T, ? extends R> mapper) {
    return map(mapper, null);
  }

  public <R> Gen<R> map(Function<? super T, ? extends R> mapper, Class<?> type) {
    return new SolrGen<>(in -> mapper.apply(generate(in)), type);
  }

  public SolrGen<T> tracked(RandomDataHistogram.Counts collector) {
    this.collector = collector;
    return this;
  }

  public SolrGen<T> withDistribution(Distribution distribution) {
    this.distribution = distribution;
    if (this.child instanceof SolrGen) {
      ((SolrGen<?>) this.child).distribution = distribution;
    }
    return this;
  }

  @Override
  public String toString() {
    return "SolrGen{"
        + "child="
        + child
        + ", type="
        + type
        + ", distribution="
        + distribution
        + '}';
  }

  protected Distribution getDistribution() {
    return this.distribution;
  }

  public class TrackingGenerator<G> extends SolrGen<G> {
    private final SolrGen<G> gen;
    private final RandomDataHistogram.Counts collector;

    public TrackingGenerator(SolrGen<G> gen, RandomDataHistogram.Counts collector) {
      this.gen = gen;
      this.collector = collector;
    }

    @Override
    public G generate(RandomnessSource in) {
      G val;
      if (in instanceof BenchmarkRandomSource) {
        val =
            gen.generate(
                (RandomnessSource) ((BenchmarkRandomSource) in).withDistribution(distribution));
        collector.collect(val);
        return val;
      }
      val = gen.generate(in);
      collector.collect(val);
      return val;
    }

    @Override
    public SolrGen<G> withDistribution(Distribution distribution) {
      if (this.child != null) {
        throw new IllegalStateException();
      }
      gen.withDistribution(distribution);

      return this;
    }

    @Override
    public String toString() {
      return "TrackingSolrGen{"
          + "wrapped="
          + gen
          + ", type="
          + type
          + ", distribution="
          + distribution
          + '}';
    }
  }
}

class SolrDescribingGenerator<G> extends SolrGen<G> {

  private final AsString<G> toString;

  public SolrDescribingGenerator(Gen<G> child, AsString<G> toString) {
    super(child);
    this.toString = toString;
  }

  @Override
  public G generate(RandomnessSource in) {
    G val;
    val = child.generate(in);
    return val;
  }

  @Override
  public G generate(SolrRandomnessSource in) {
    G val;
    val = child.generate(in);
    return val;
  }

  @Override
  public String asString(G t) {
    return toString.asString(t);
  }
}
