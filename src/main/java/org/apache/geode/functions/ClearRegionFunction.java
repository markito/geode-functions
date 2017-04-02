package org.apache.geode.functions;
/*
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

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;

import org.apache.geode.cache.partition.PartitionRegionHelper;

import java.util.Iterator;
import java.util.Properties;

public class ClearRegionFunction implements Function, Declarable {

  public void execute(FunctionContext context) {
    System.out.println(Thread.currentThread().getName() + ": Executing " + getId());
    RegionFunctionContext rfc = (RegionFunctionContext) context;
    Region localRegion = PartitionRegionHelper.getLocalDataForContext(rfc);
    int numLocalEntries = localRegion.size();
    
    // Destroy each entry
    long start=0, end=0;
    start = System.currentTimeMillis();
    for (Iterator i = localRegion.keySet().iterator(); i.hasNext();) {
      i.next();
      i.remove();
    }
    end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + ": Cleared " + numLocalEntries + " entries in " + (end-start) + " ms");
    context.getResultSender().lastResult(true);
  }

  public String getId() {
    return getClass().getSimpleName();
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean hasResult() {
    return true;
  }

  public boolean isHA() {
    return true;
  }

  public void init(Properties properties) {
  }
}
