package org.apache.geode.functions;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.jgroups.util.Util.assertFalse;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;

/**
 * @TODO Consider JUnit Parameterization
 * @TODO Tests for multiple cache servers
 */
public class CopyRegionTest {

  public static final String FUNCTION_ID = "CopyRegion";
  public static final String REGION_A = "regionA";
  public static final String REGION_B = "regionB";

  private Cache cache;
  private CopyRegion copyRegionFunction;
  private Region regionA;
  private Region regionB;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  public static final int NUM_ENTRIES = 10;

  @Before
  public void setup() {
    cache = new CacheFactory().create();
    copyRegionFunction = new CopyRegion();
  }

  @After
  public void cleanUp() {
    cache.close();
  }

  private void populateRegion(final int numEntries, Region region) {
    for (int i = 0; i < numEntries; i++) {
      region.put("key " + i, "value " + i);
    }
    assertTrue(region.size() == numEntries);
  }

  private void createRegions() {
    regionA = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_A);
    regionB = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_B);
  }

  @Test
  public void functionIsSuccessfullyRegistered() {
    assertTrue(FunctionService.getFunction(FUNCTION_ID) == null);
    FunctionService.registerFunction(copyRegionFunction);
    assertFalse(FunctionService.getFunction(FUNCTION_ID) == null);
  }

//  @Test
//  public void executeCopyFunctionOnReplicatedRegionShouldFail() throws Exception {
//    createReplicatedRegions();
//    populateRegion(NUM_ENTRIES, regionA);
//
//    FunctionService.registerFunction(copyRegionFunction);
//    assertFalse(FunctionService.getFunction(FUNCTION_ID) == null);
//
//    thrown.expect(IllegalArgumentException.class);
//    FunctionService.onRegion(regionA).withArgs(REGION_B).execute(FUNCTION_ID);
//
////    assertThat(regionA.size(), is(equalTo(regionB.size())));
//  }

  @Test
  public void executeCopyFunctionOnPartitionRegion() throws Exception {
    createRegions();
    populateRegion(NUM_ENTRIES, regionA);

    FunctionService.registerFunction(copyRegionFunction);
    assertFalse(FunctionService.getFunction(FUNCTION_ID) == null);

    FunctionService.onRegion(regionA).withArgs(REGION_B).execute(FUNCTION_ID);
    assertThat(regionA.size(), is(equalTo(regionB.size())));
  }

    @Test
    public void executeCopyFunctionOnMemberShouldThrowException() throws Exception {
      int numEntries = 10;

      Region regionA = cache.createRegionFactory(RegionShortcut.PARTITION).create(REGION_A);

      populateRegion(numEntries, regionA);
      assertTrue(regionA.size() == numEntries);

      CopyRegion fn = new CopyRegion();
      FunctionService.registerFunction(fn);
      assertFalse(FunctionService.getFunction(FUNCTION_ID) == null);

      FunctionService.onMembers(cache.getDistributedSystem()).execute(FUNCTION_ID);

    }

  @Test
  public void executeCopyFunctionWithNonExistingRegionShouldFail() throws Exception {

    CopyRegion fn = new CopyRegion();
    FunctionService.registerFunction(fn);

    assertFalse(FunctionService.getFunction(FUNCTION_ID) == null);
    thrown.expect(FunctionException.class);

    FunctionService.onRegion(regionA).withArgs(REGION_B).execute(FUNCTION_ID);

  }

}