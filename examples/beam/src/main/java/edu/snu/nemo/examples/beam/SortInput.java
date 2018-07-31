/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.examples.beam;

import com.google.common.collect.Lists;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineOptions;
import edu.snu.nemo.compiler.frontend.beam.NemoPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * IP stat analysis example used in skew experiment.
 */
public final class SortInput {
  private static final Logger LOG = LoggerFactory.getLogger(SortInput.class.getName());

  /**
   * Private Constructor.
   */
  private SortInput() {
  }

  /**
   * Main function for the MR BEAM program.
   * @param args arguments.
   */
  public static void main(final String[] args) {
    final String inputFilePath = args[0];
    final String outputFilePath = args[1];
    final PipelineOptions options = PipelineOptionsFactory.create().as(NemoPipelineOptions.class);
    options.setRunner(NemoPipelineRunner.class);
    options.setJobName("SortInput");

    final Pipeline p = Pipeline.create(options);

    long start = System.currentTimeMillis();

    final PCollection<String> result = GenericSourceSink.read(p, inputFilePath)
        .apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
          @Override
          public KV<String, String> apply(final String line) {
            final String[] words = line.split(" ");
            String key = words[1];  // value becomes the key
            String value = words[0];
            return KV.of(key, value);
          }
        }))
        .apply(GroupByKey.create())
        .apply(MapElements.via(new SimpleFunction<KV<String, Iterable<String>>, KV<String, List<String>>>() {
          @Override
          public KV<String, List<String>> apply(final KV<String, Iterable<String>> kv) {
            final String value = kv.getKey();
            List keys = Lists.newArrayList(kv.getValue());
            Collections.sort(keys);
            return KV.of(value, keys);
          }
        }))
        .apply(MapElements.via(new SimpleFunction<KV<String, List<String>>, String>() {
          @Override
          public String apply(final KV<String, List<String>> valueTokeys) {
            String ret = "";
            String value = valueTokeys.getKey();
            List<String> keys = valueTokeys.getValue();
            for (String key : keys) {
              ret = ret + value + " " + key + "\n";
            }
            return ret;
          }
        }));
    GenericSourceSink.write(result, outputFilePath);
    p.run();

    LOG.info("*******END*******");
    LOG.info("JCT(ms): " + (System.currentTimeMillis() - start));
  }
}
