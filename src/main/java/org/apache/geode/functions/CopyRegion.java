package org.apache.geode.functions;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.logging.LogService;

public class CopyRegion implements Function, Declarable {

  private static final int BATCH_SIZE = 1000;
  private LongAdder keyCounter = new LongAdder();
  Logger logger = LogService.getLogger();

  @Override
  public void execute(final FunctionContext context) {
    logger.info("Running...");

    if(! (context instanceof RegionFunctionContext))
      throw new FunctionException("This is a data aware function and has to be called using FunctionService.onRegion.");

    try {
      Cache cache = CacheFactory.getAnyInstance();
      RegionFunctionContext rfc = (RegionFunctionContext) context;
      final String toRegionName = extractToRegionName(context);

      final Region toRegion = cache.getRegion(toRegionName);
      LocalDataSet fromRegion = (LocalDataSet) PartitionRegionHelper.getLocalDataForContext(rfc);

      if (!checkArguments(toRegionName, toRegion, fromRegion)) {
        logger.error("Error validating arguments.");
      } else {
        Set<Entry> localEntrySet = fromRegion.localEntrySet();
        ConcurrentHashMap entryBatch = new ConcurrentHashMap();
        Iterator iterator = localEntrySet.iterator();

        while (iterator.hasNext()) {
          LocalRegion.NonTXEntry entry = (LocalRegion.NonTXEntry) iterator.next();

          entryBatch.put(entry.getKey(), entry.getValue());
          if (( entryBatch.size() % BATCH_SIZE) == 0) {
            putBatch(toRegion, entryBatch);
          }
        }

        if (entryBatch.size() > 0) {
          putBatch(toRegion, entryBatch);
        }
        logger.info(String.format("Done. Copied %d keys from %s to %s region [%d].", keyCounter.longValue(), fromRegion.getName(), toRegion.getName(), toRegion.keySet().size()));
      }
    } catch (RuntimeException exception) {
      exception.printStackTrace();
      throw exception;
    }

    //@TODO: custom spliterator needed for batching
    //        fromRegion.localEntrySet().parallelStream().forEach(k -> entryBatch.put(k.getKey(), k.getValue()));
    //        //TODO: still looks like race could happen, need something better ?
    //        while (entryBatch.size() > 0) {
    //          keyCounter.add(entryBatch.size());
    //          toRegion.putAll(entryBatch);
    //          entryBatch.clear();
    //        }

    //    context.getResultSender().lastResult("ok!");
  }

  private void putBatch(final Region toRegion, final ConcurrentHashMap entryBatch) {
    keyCounter.add(entryBatch.size());
    toRegion.putAll(entryBatch);
    entryBatch.clear();
  }

  private String extractToRegionName(final FunctionContext context) {
    final String toRegionName;
    if (context.getArguments() instanceof String[]) {
      String[] arguments = (String[]) context.getArguments();
      toRegionName = arguments[0];
    } else {
      toRegionName = (String) context.getArguments();
    }
    return toRegionName;
  }

  private boolean checkArguments(final String toRegionName, final Region toRegion, final Region fromRegion) {
    if ((toRegionName == null) || (toRegionName.isEmpty())) {
      throw new IllegalArgumentException("Missing argument for CopyFunction: Region name.");
    } else if ((toRegion == null) || (fromRegion == null)) {
      throw new IllegalStateException("Either fromRegion or toRegion are not available on the system. Make sure regions exists on the system before initiating the copy.");
    } else {
      return true;
    }
  }

  @Override
  public String getId() {
    return "CopyRegion";
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public boolean hasResult() {
    return false;
  }

  @Override
  public void init(final Properties props) {

  }
}
