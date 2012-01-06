package com.nexr.cache;

public class Names {

    public static final String ROOT = "/root";

    public static final String MANAGER = "/manager";
    public static final String CONFIG = "/config";
    public static final String RUNTIME = "/runtime";

    private static final String MANAGER_PREFIX = "/manager/";
    private static final String CONFIG_PREFIX = "/config/";
    private static final String RUNTIME_PREFIX = "/runtime/";

    private static final String SERVER = "/servers";
    private static final String NAMESPACE = "/namespaces";

    private static final String SERVER_POSTFIX = "/servers/";
    private static final String NAMESPACE_POSTFIX = "/namespaces/";

    public static String managerPath(String cluster) {
        return MANAGER_PREFIX + cluster;
    }

    public static String configPath(String cluster) {
        return CONFIG_PREFIX + cluster;
    }

    public static String serverConfigPath(String cluster) {
        return CONFIG_PREFIX + cluster + SERVER;
    }

    public static String nspaceConfigPath(String cluster) {
        return CONFIG_PREFIX + cluster + NAMESPACE;
    }

    public static String runtimePath(String cluster) {
        return RUNTIME_PREFIX + cluster;
    }

    public static String serverRuntimePath(String cluster) {
        return RUNTIME_PREFIX + cluster + SERVER;
    }

    public static String nspaceRuntimePath(String cluster) {
        return RUNTIME_PREFIX + cluster + NAMESPACE;
    }

    public static String serverRuntimePath(String cluster, String server) {
        return RUNTIME_PREFIX + cluster + SERVER_POSTFIX + server;
    }

    public static String nspaceRuntimePath(String cluster, String namespace) {
        return RUNTIME_PREFIX + cluster + NAMESPACE_POSTFIX + namespace;
    }

    public static String configPath(String cluster, IndexedServerName server) {
        return serverConfigPath(cluster) + "/" + server.index() + ":" + server.name();
    }

    public static String configPath(String cluster, NameSpace namespace) {
        return nspaceConfigPath(cluster) + "/" + namespace.serialize();
    }
}
