package cassandra.meteo;

import config.Config;
import entity.MeteorologicalData;
import exception.ImportException;
import filegen.FileGenerator;
import filegen.GenerateFile;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.xml.transform.Result;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static query.cassandra.QueryEngine.LATEST_QUERY;

public class DataImporter {

    private Cassandra.Client client;

    public DataImporter(String serverIP, int serverPort) throws ImportException {
        TTransport tr = new TSocket(serverIP, serverPort);
        TFramedTransport tf = new TFramedTransport(tr);
        TProtocol proto = new TBinaryProtocol(tf);
        client = new Cassandra.Client(proto);
        try {
            tr.open();
        } catch (TTransportException e) {
            throw new ImportException(e);
        }
        try {
            client.set_keyspace(Config.KS_NAME);
        } catch (TException e) {
            throw new ImportException(e);
        }
    }

    public void insert(MeteorologicalData data) throws ImportException {
        insertData(data);
        insertLatest(data);
        insertLayerIndex(data);
        insertIntervalIndex(data);
    }

    private void insertData(MeteorologicalData data) throws ImportException {
        ColumnParent columnParent = new ColumnParent(data.pattern + Config.DATA_CF_SUFFIX);
        String rowkey = data.measurement + FileGenerator.LEVEL_SEPARATOR + data.layer;
        String columnName = data.date + FileGenerator.LEVEL_SEPARATOR + data.interval;
        Column column = new Column(ByteBuffer.wrap(columnName.getBytes()));
        column.setValue(data.content);
        column.setTimestamp(System.currentTimeMillis());
        try {
            client.insert(ByteBuffer.wrap(rowkey.getBytes()), columnParent, column, Config.CASSANDRA_CONSISTENCY_LEVEL);
        } catch (TException e) {
            throw new ImportException(e);
        }
    }

    private void insertLatest(MeteorologicalData data) throws ImportException {
        if (!Config.DATA_IN_ORDER) {
            String CFName = data.pattern + Config.LATEST_CF_SUFFIX;
            String rowkey = data.measurement + GenerateFile.LEVEL_SEPARATOR + data.layer;
            String columnName = data.interval;
            String sql = String.format(LATEST_QUERY, columnName, CFName, rowkey);
            try {
                CqlResult result = client.execute_cql_query(ByteBuffer.wrap(sql.getBytes()) ,Compression.NONE);
                Iterator<CqlRow> rowIterator = result.getRowsIterator();
                if (rowIterator.hasNext()) {
                    CqlRow row = rowIterator.next();
                    String oldLatest = new String(row.getColumns().get(0).value.array());
                    if (oldLatest.compareTo(data.date) > 0)
                        return;
                }
            } catch (TException e) {
                throw new ImportException("Cannot query latest time during insertion", e);
            }
        }

        ColumnParent columnParent = new ColumnParent(data.pattern + Config.LATEST_CF_SUFFIX);
        String rowkey = data.measurement + FileGenerator.LEVEL_SEPARATOR + data.layer;
        String columnName = data.interval;
        Column column = new Column(ByteBuffer.wrap(columnName.getBytes()));
        column.setValue(data.date.getBytes());
        column.setTimestamp(System.currentTimeMillis());
        try {
            client.insert(ByteBuffer.wrap(rowkey.getBytes()), columnParent, column, Config.CASSANDRA_CONSISTENCY_LEVEL);
        } catch (TException e) {
            throw new ImportException(e);
        }
    }

    private void insertLayerIndex(MeteorologicalData data) throws ImportException {
        ColumnParent columnParent = new ColumnParent(data.pattern + Config.LAYER_INDEX_CF_SUFFIX);
        String rowkey = data.measurement;
        String columnName = data.layer;
        Column column = new Column(ByteBuffer.wrap(columnName.getBytes()));
        column.setValue("".getBytes());
        column.setTimestamp(System.currentTimeMillis());
        try {
            client.insert(ByteBuffer.wrap(rowkey.getBytes()), columnParent, column, Config.CASSANDRA_CONSISTENCY_LEVEL);
        } catch (TException e) {
            throw new ImportException(e);
        }
    }

    private void insertIntervalIndex(MeteorologicalData data) throws ImportException {
        ColumnParent columnParent = new ColumnParent(data.pattern + Config.INTERVAL_INDEX_CF_SUFFIX);
        String rowkey = data.measurement + FileGenerator.LEVEL_SEPARATOR + data.layer + FileGenerator.LEVEL_SEPARATOR + data.date;
        String columnName = data.interval;
        Column column = new Column(ByteBuffer.wrap(columnName.getBytes()));
        column.setValue("".getBytes());
        column.setTimestamp(System.currentTimeMillis());
        try {
            client.insert(ByteBuffer.wrap(rowkey.getBytes()), columnParent, column, Config.CASSANDRA_CONSISTENCY_LEVEL);
        } catch (TException e) {
            throw new ImportException(e);
        }
    }
}
