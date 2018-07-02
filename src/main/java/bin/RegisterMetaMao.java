package bin;

import cassandra.meteo.MetaRegister;
import config.Config;

public class RegisterMetaMao {
    private MetaRegister register;
    private String[] patterns;


    public RegisterMetaMao(String serverIP, int port, String username, String pw, String[] patterns) {
        register = new MetaRegister(serverIP, port, username, pw);
        this.patterns = patterns;
    }

    public void registerAll() {

        for (String CFName : patterns) {
            // data CFs
            register.createCF(CFName + Config.DATA_CF_SUFFIX);
            // latest date CFs
            register.createCF(CFName + Config.LATEST_CF_SUFFIX);
            // index CFs
            register.createCF(CFName + Config.LAYER_INDEX_CF_SUFFIX);
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
        RegisterMetaMao registerMeta = new RegisterMetaMao(Config.CASSANDRA_IP, Config.CASSANDRA_PORT, Config.CASSANDRA_USER,
                Config.CASSANDRA_PW, Config.PATTERNS);
        registerMeta.registerAll();
    }
}
