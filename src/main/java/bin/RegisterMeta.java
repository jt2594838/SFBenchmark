package bin;

import cassandra.meteo.MetaRegister;
import config.Config;

public class RegisterMeta {

    private MetaRegister register;
    private String[] levelPrefixes;
    private int[] instanceNumbers;

    public RegisterMeta(String serverIP, int port, String username, String pw, String[] levelPrefixes, int[] instanceNumbers) {
        register = new MetaRegister(serverIP, port, username, pw);
        this.levelPrefixes = levelPrefixes;
        this.instanceNumbers = instanceNumbers;
    }

    public void registerAll() {
        // data CFs
        for (int i = 0; i < instanceNumbers[0]; i++) {
            String CFName = levelPrefixes[0];
            register.createCF(CFName + Config.DATA_CF_SUFFIX);
        }
        // latest date CFs
        for (int i = 0; i < instanceNumbers[0]; i++) {
            String CFName = levelPrefixes[0];
            register.createCF(CFName + Config.LATEST_CF_SUFFIX);
        }
        // index CFs
        for (int i = 0; i < instanceNumbers[0]; i++) {
            String CFName = levelPrefixes[0];
            register.createCF(CFName + Config.LAYER_INDEX_CF_SUFFIX);
        }
        for (int i = 0; i < instanceNumbers[0]; i++) {
            String CFName = levelPrefixes[0];
            register.createCF(CFName + Config.INTERVAL_INDEX_CF_SUFFIX);
        }
    }

    private static void help() {
        System.out.println("Usage: [<config file path>]");
    }

    /*
    Usage: [<config file path>]
     */
    public static void main(String[] args) {
        if (args.length == 1) {
            Config.loadConfig(args[0]);
        } else if (args.length != 0) {
            help();
            return;
        }
        RegisterMeta registerMeta = new RegisterMeta(Config.CASSANDRA_IP, Config.CASSANDRA_PORT, Config.CASSANDRA_USER,
                Config.CASSANDRA_PW, Config.LEVER_PREFIXES, Config.INSTANCE_NUMBERS);
        registerMeta.registerAll();
    }
}
