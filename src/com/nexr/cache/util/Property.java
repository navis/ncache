package com.nexr.cache.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Property {

    public static final String MPOOL_START = "memory.start";
    public static final String MPOOL_INCREASE = "memory.increase";
    public static final String MPOOL_SLABSIZE = "memory.slabsize";

    public static final String PPOOL_START = "storage.start";
    public static final String PPOOL_INCREASE = "storage.increase";
    public static final String PPOOL_SLABSIZE = "storage.slabsize";
    public static final String STORAGE_DIRECTORY = "storage.directory";

    public static final String THREAD_POOL = "thread.pool";

    public static final int MPOOL_START_DEFAULT = 32;
    public static final float MPOOL_INCREASE_DEFAULT = 0.25f;
    public static final int MPOOL_SLABSIZE_DEFAULT = 4 << 20;

    public static final int PPOOL_START_DEFAULT = 256;
    public static final float PPOOL_INCREASE_DEFAULT = 0.33f;
    public static final int PPOOL_SLABSIZE_DEFAULT = Integer.MAX_VALUE;

    public static final int THREAD_POOL_DEFAULT = 20;

    Properties props = new Properties(defaultProps());

    public Property(String config) throws IOException {
        if (config != null && !config.equals("null")) {
            props.load(new FileInputStream(config));
        }
    }

    public String get(String key) {
        return props.getProperty(key);
    }

    public int getInt(String key) {
        return Integer.valueOf(props.getProperty(key));
    }

    public int getInt(String key, String defKey) {
        return Integer.valueOf(props.getProperty(key, props.getProperty(defKey)));
    }

    public float getFloat(String key) {
        return Float.valueOf(props.getProperty(key));
    }

    public float getFloat(String key, String defKey) {
        return Float.valueOf(props.getProperty(key, props.getProperty(defKey)));
    }

    private static Properties defaultProps() {
        Properties props = new Properties();
        props.setProperty(MPOOL_START, String.valueOf(MPOOL_START_DEFAULT));
        props.setProperty(MPOOL_INCREASE, String.valueOf(MPOOL_INCREASE_DEFAULT));
        props.setProperty(MPOOL_SLABSIZE, String.valueOf(MPOOL_SLABSIZE_DEFAULT));
        props.setProperty(PPOOL_START, String.valueOf(PPOOL_START_DEFAULT));
        props.setProperty(PPOOL_INCREASE, String.valueOf(PPOOL_INCREASE_DEFAULT));
        props.setProperty(PPOOL_SLABSIZE, String.valueOf(PPOOL_SLABSIZE_DEFAULT));
        props.setProperty(THREAD_POOL, String.valueOf(THREAD_POOL_DEFAULT));
        return props;
    }
}
