package cassandra.meteo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import config.Config;

/**
 * This class creates keyspace and column families.
 */
public class MetaRegister {
    private Cluster cluster;
    private Session session;

    public MetaRegister(String serverIP, int serverPort, String username, String password) {
        cluster = Cluster.builder().addContactPoint(serverIP).withPort(serverPort)
                .withCredentials(username, password).build();
        session = cluster.connect();
        createKeyspace();
        session.execute("USE " + Config.KS_NAME);
    }

    private void createKeyspace() {
        String cql = "CREATE KEYSPACE if not exists " + Config.KS_NAME
                + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '" + Config.REPLICATION_FACOTR + "'}";
        session.execute(cql);
    }

    public void createCF(String CFName) {
        String cql = "CREATE COLUMNFAMILY if not exists " + CFName + " (rowkey text PRIMARY KEY) WITH comparator=text AND default_validation=BytesType";
        session.execute(cql);
    }
}
