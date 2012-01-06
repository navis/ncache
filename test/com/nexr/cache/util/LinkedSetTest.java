package com.nexr.cache.util;

import org.junit.Test;

import junit.framework.Assert;
import com.nexr.cache.TestUtils;

public class LinkedSetTest {

    @Test
    public void testBasic() {
        LinkedSet<String> set = new LinkedSet<String>(true);
        Assert.assertNull(set.put("navis"));
        Assert.assertEquals("navis", set.get("navis"));
        Assert.assertEquals("navis", set.remove("navis"));

        set.put("navis1");
        set.put("navis2");
        set.put("navis3");
        set.put("navis4");

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis2", "navis3", "navis4"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis3", "navis4"));

        Assert.assertEquals("navis2", set.get("navis2"));

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis3", "navis4", "navis2"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis3", "navis4"));

        Assert.assertEquals("navis3", set.removeMark("navis3"));

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis3", "navis4", "navis2"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis4", "navis3"));

        set.put("navis5");

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis3", "navis4", "navis2", "navis5"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis4", "navis3", "navis5"));

        Assert.assertNull(set.get("navis3"));

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis3", "navis4", "navis2", "navis5"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis4", "navis3", "navis5"));

        Assert.assertEquals("navis3", set.put("navis3"));

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis4", "navis2", "navis5", "navis3"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis4", "navis5", "navis3"));

        Assert.assertEquals("navis3", set.remove("navis3"));

        Assert.assertTrue(TestUtils.equals(set.values(), "navis1", "navis4", "navis2", "navis5"));
        Assert.assertTrue(TestUtils.equals(set.cvalues(), "navis1", "navis2", "navis4", "navis5"));
    }
}
