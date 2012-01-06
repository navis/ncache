package com.nexr.cache.cluster;

import org.junit.Test;

import junit.framework.Assert;
import com.nexr.cache.util.Generations;

public class StampGeneratorTest {

    @Test
    public void testStamp() {
        Generations generator = new Generations(1);
        Assert.assertEquals(0x100001, generator.next());

        generator.set(Generations.COUNT_MASK - 1);
        Assert.assertEquals(0x1fffff, generator.next());
        Assert.assertEquals(0x100000, generator.next());
    }
}
