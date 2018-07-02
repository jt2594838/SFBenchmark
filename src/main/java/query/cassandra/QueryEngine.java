package query.cassandra;

import com.datastax.driver.core.*;
import config.Config;
import entity.MeteorologicalData;
import filegen.FileGenerator;
import filegen.GenerateFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryEngine {

    public static final String DATA_QUERY = "select %s from %s where rowkey = %s";
    public static final String LATEST_QUERY = "select %s from %s where rowkey = %s";
    public static final String LAYER_QUERY = "select * from %s where rowkey = %s";
    public static final String INTERVAL_QUERY = "select * from %s where rowkey = %s";
    private Cluster cluster;
    private Session session;

    public QueryEngine(String serverIP, int serverPort, String username, String password) {
        cluster = Cluster.builder().addContactPoint(serverIP).withPort(serverPort)
                .withCredentials(username, password).build();
        session = cluster.connect();
        session.execute("USE " + Config.KS_NAME);
    }

    public MeteorologicalData queryData(MeteorologicalData data) {
        String CFName = data.pattern + Config.DATA_CF_SUFFIX;
        String rowkey = data.measurement + GenerateFile.LEVEL_SEPARATOR + data.layer;
        String columnName = data.date + GenerateFile.LEVEL_SEPARATOR + data.interval;
        String sql = String.format(DATA_QUERY, columnName, CFName, rowkey);

        ResultSet resultSet = session.execute(sql);
        Row result = resultSet.one();
        if (result == null) {
            data.content = null;
        } else {
            data.content = result.getBytes(columnName).array();
        }
        return data;
    }

    public MeteorologicalData queryLatest(MeteorologicalData data) {
        String CFName = data.pattern + Config.LATEST_CF_SUFFIX;
        String rowkey = data.measurement + GenerateFile.LEVEL_SEPARATOR + data.layer;
        String columnName = data.interval;
        String sql = String.format(LATEST_QUERY, columnName, CFName, rowkey);

        ResultSet resultSet = session.execute(sql);
        Row result = resultSet.one();
        if (result == null) {
            data.date = null;
        } else {
            data.date = new String(result.getBytes(columnName).array());
        }
        return queryData(data);
    }

    private List<String> queryLayers(MeteorologicalData data) {
        String CFName = data.pattern + Config.LAYER_INDEX_CF_SUFFIX;
        String rowkey = data.measurement;
        String sql = String.format(LAYER_QUERY, CFName, rowkey);

        List<String> ret = new ArrayList<>();
        ResultSet resultSet = session.execute(sql);
        ColumnDefinitions definitions = resultSet.getColumnDefinitions();
        for(ColumnDefinitions.Definition definition : definitions) {
            ret.add(definition.getName());
        }
        ret.sort(null);
        return ret;
    }

    public MeteorologicalData queryNextLayer(MeteorologicalData data) {
        List<String> layers = queryLayers(data);
        int currIndex = Collections.binarySearch(layers, data.layer);
        data.content = null;
        if(currIndex == layers.size() - 1 || currIndex < 0) {
            return data;
        } else {
            data.layer = layers.get(currIndex + 1);
            return queryData(data);
        }
    }

    public MeteorologicalData queryPrevLayer(MeteorologicalData data) {
        List<String> layers = queryLayers(data);
        int currIndex = Collections.binarySearch(layers, data.layer);
        data.content = null;
        if(currIndex == 0 || currIndex < 0) {
            return data;
        } else {
            data.layer = layers.get(currIndex - 1);
            return queryData(data);
        }
    }

    private List<String> queryIntervals(MeteorologicalData data) {
        String CFName = data.pattern + Config.INTERVAL_INDEX_CF_SUFFIX;
        String rowkey = data.measurement + FileGenerator.LEVEL_SEPARATOR + data.layer + FileGenerator.LEVEL_SEPARATOR + data.date;
        String sql = String.format(INTERVAL_QUERY, CFName, rowkey);

        List<String> ret = new ArrayList<>();
        ResultSet resultSet = session.execute(sql);
        ColumnDefinitions definitions = resultSet.getColumnDefinitions();
        for(ColumnDefinitions.Definition definition : definitions) {
            ret.add(definition.getName());
        }
        ret.sort(null);
        return ret;
    }

    public MeteorologicalData queryNextInterval(MeteorologicalData data) {
        List<String> layers = queryIntervals(data);
        int currIndex = Collections.binarySearch(layers, data.layer);
        data.content = null;
        if(currIndex == layers.size() - 1 || currIndex < 0) {
            return data;
        } else {
            data.layer = layers.get(currIndex + 1);
            return queryData(data);
        }
    }

    public MeteorologicalData queryPrevInterval(MeteorologicalData data) {
        List<String> layers = queryIntervals(data);
        int currIndex = Collections.binarySearch(layers, data.layer);
        data.content = null;
        if(currIndex == 0 || currIndex < 0) {
            return data;
        } else {
            data.layer = layers.get(currIndex - 1);
            return queryData(data);
        }
    }
}
