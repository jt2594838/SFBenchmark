package config;

import org.apache.cassandra.thrift.ConsistencyLevel;

public class Config {
    public static final String DATA_CF_SUFFIX = "_data";
    public static final String LATEST_CF_SUFFIX = "_newest";
    public static final String LAYER_INDEX_CF_SUFFIX = "layer_index";
    public static final String INTERVAL_INDEX_CF_SUFFIX = "interval_index";

    public static String KS_NAME = "benchmark_ks";
    public static boolean DATA_IN_ORDER = true;

    // file generator params
    public static String[] LEVER_PREFIXES = {"P","M","L","T","I"};

    public static int MAX_ENTRY_PER_FILE = 1000000;

    public static int FILE_SIZE_IN_BYTE = 819200;

    public static String COMPACT_OUTPUT = "output/compact";

    public static int[] INSTANCE_NUMBERS = {10,80,50,6,80};

    // mao's gen
    public static int[] LEVEL_NUM = {10, 10, 10, 2, 5};

    public static int[] OFFSETS = {0, 0, 0, 0, 0};

    public static int[] LENGTHS = {10, 10, 10, 2, 5};

    public static int PATTERN_LEN = 5;

    // cassandra params
    public static int REPLICATION_FACOTR = 1;

    public static String CASSANDRA_IP = "";

    public static int CASSANDRA_PORT = 0;

    public static String CASSANDRA_USER = "";

    public static String CASSANDRA_PW = "";

    public static ConsistencyLevel CASSANDRA_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

    // for mao's path
    public static String[] PATTERNS;

    static {
        PATTERNS = new String[LEVEL_NUM[0]];
        for (int i = 0; i < LEVEL_NUM[0]; i++) {
            PATTERNS[i] = "";
            for (int j = 0; j < PATTERN_LEN; j++) {
                PATTERNS[i] += (char) ('a' + i);
            }
        }
    }

    public static void loadConfig(String fileName) {

    }
}
