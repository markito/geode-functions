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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;

public class ClearRegionFunctionsTest {

  private Cache cache;
  private Region region;

  public static final int NUM_ENTRIES = 1000;
  public static final String REGION_NAME = "data";

  @Before
  public void setup() {
    this.cache = new CacheFactory().create();
  }

  @After
  public void cleanUp() {
    this.cache.close();
  }

  private void populateRegion(int numEntries) {
    for (int i = 0; i < numEntries; i++) {
      this.region.put("key " + i, "value " + i);
    }
    assertTrue(region.size() == numEntries);
  }

  private void createRegion() {
    this.region = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }
  
  private void initializeRegionAndFunction(Function function) {
    createRegion();
    populateRegion(NUM_ENTRIES);
    FunctionService.registerFunction(function);
    assertFalse(FunctionService.getFunction(function.getId()) == null);
  }

  @Test
  public void executeClearRegionFunction() throws Exception {
    executeFunction(new ClearRegionFunction());
  }

  @Test
  public void executeClearRegionRemoveAllFunction() throws Exception {
    executeFunction(new ClearRegionRemoveAllFunction());
  }
  
  private void executeFunction(Function function) {
    initializeRegionAndFunction(function);
    FunctionService.onRegion(this.region).execute(function);
    assertEquals(0, this.region.size());
  }
}
