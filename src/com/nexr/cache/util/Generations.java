package com.nexr.cache.util;

import java.util.concurrent.atomic.AtomicLong;

import static com.nexr.cache.Constants.INFO_GSHIGH;
import static com.nexr.cache.Constants.INFO_GSLOW;

public class Generations {

    public static final long SERVER_MASK = 0xfff0000000000000l;
    public static final int SERVER_MASK_HIGH = 0xfff00000;

    public static final long COUNT_MASK = 0x000fffffffffffffl;
    public static final int COUNT_MASK_HIGH = 0x000fffff;

    private final long server;
    private final AtomicLong generator;

    public Generations(int index) {
        server = (long) index << 52;
        generator = new AtomicLong();
    }

    public long next() {
        return server + (generator.incrementAndGet() & COUNT_MASK);
    }

    public void set(long generation) {
        generator.set(generation);
    }

    public static int[] INDEX(int server, long counter, int hash) {
        long generation = ((long)server << 52) + (counter & COUNT_MASK);
        return new int[] {(int) (generation >> 32), (int) generation, hash};
    }

    public static int[] INDEX(long generation, int hash) {
        return new int[] {(int) (generation >> 32), (int) generation, hash};
    }

    public static int[] INDEX(int genHigh, int genLow, int hash) {
        return new int[] {genHigh, genLow, hash};
    }

    public static int GENERATION(int[] generation) {
        return generation[2];
    }

    public static int HASH(int[] generation) {
        return generation[2];
    }

    public static int SERVER(int[] generation) {
        return generation[0] >> 20;
    }

    public static int SERVER(long generation) {
        return (int) (generation >> 52);
    }

    public static long COUNTER(int[] generation) {
        return (((long)generation[0] & COUNT_MASK_HIGH) << 32) + generation[1];
    }

    public static long COUNTER(long generation) {
        return generation & COUNT_MASK;
    }

    public static String TO_STRING(int[] generation) {
        return SERVER(generation) + ":" + COUNTER(generation);
    }

    public static boolean SAME_AGE(long generation1, long generation2) {
        return (generation1 & COUNT_MASK) == (generation2 & COUNT_MASK);
    }

    public static boolean OLDER(int[] generation1, int[] generation2) {
        return generation1[INFO_GSHIGH] >= generation2[INFO_GSHIGH] && generation1[INFO_GSLOW] > generation2[INFO_GSLOW];
    }

    public static boolean OLDER(long generation1, long generation2) {
        return (generation1 & COUNT_MASK) > (generation2 & COUNT_MASK);
    }

    public static boolean YOUNGER(long generation1, long generation2) {
        return OLDER(generation2, generation1);
    }

    public static boolean SAME_SERVER(int[] generation1, int[] generation2) {
        return (generation1[0] & SERVER_MASK_HIGH) == (generation2[0] & SERVER_MASK_HIGH);
    }

    public static boolean SAME_SERVER(long generation1, long generation2) {
        return (generation1 & SERVER_MASK) == (generation2 & SERVER_MASK);
    }

    public static long NEW(int server, long counter) {
        return ((long)server << 52) + (counter & COUNT_MASK);
    }

    public static String DUMP_ALL(long[][] generations) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < generations.length; i++) {
            if (generations[i][0] == 0 && generations[i][1] == 0 && generations[i][2] == 0) {
                continue;
            }
            builder.append("partition ").append(i).append('=').append(generations[i][0]).append(':').append(generations[i][1]).append(':').append(generations[i][2]);
            builder.append('\n');
        }
        return builder.toString();
    }
}
