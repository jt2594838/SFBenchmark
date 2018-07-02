package bin;

import cassandra.meteo.DataImporter;
import config.Config;
import entity.MeteorologicalData;
import exception.FormatException;
import exception.ImportException;
import filegen.GenerateFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ImportCassandra {
    private static final Logger logger = LoggerFactory.getLogger(ImportCassandra.class);
    private DataImporter importer;
    private GenerateFile generator;

    public ImportCassandra(String serverIP, int serverPort, String seedPath) throws ImportException {
        importer = new DataImporter(serverIP, serverPort);
        try {
            generator = new GenerateFile(seedPath);
        } catch (IOException e) {
            throw new ImportException(e);
        }
    }

    public void doImport() {
        int count = 0;
        long batchStartTime = System.currentTimeMillis();
        long totalTime = 0;
        int batchSize = 100000;

        while (generator.hasNext()) {
            generator.next();
            count++;
            MeteorologicalData data;
            try {
                data = new MeteorologicalData(generator.getCurrPath(), generator.getContentSeed());
            } catch (FormatException e) {
                logger.error("Path format {} is not right.", generator.getCurrPath(), e);
                return;
            }
            try {
                importer.insert(data);
            } catch (ImportException e) {
                logger.error("Cannot insert {}.", generator.getCurrPath(), e);
                return;
            }
            if ((count % batchSize) == 0) {
                long batchTime = System.currentTimeMillis() - batchStartTime;
                totalTime += batchTime;
                logger.info("{} inserted, batch time {}, total time {}", count, batchTime, totalTime);
                batchStartTime = System.currentTimeMillis();
            }
        }
        if ((count % batchSize) != 0) {
            long batchTime = System.currentTimeMillis() - batchStartTime;
            totalTime += batchTime;
            logger.info("{} inserted, batch time {}, total time {}", count, batchTime, totalTime);
        }
    }

    public static void help() {
        System.out.println("Usage:\n" +
                "        args:  {IP} {port} {seedPath} [{offsets}] [{lengths}]\n" +
                "        example: 127.0.0.1 6667 res/seed.txt 0,0,0,0,0  10,80,50,6,80");
    }

    /*
     * Usage:
     *   args:  {IP} {port} {seedPath} [{offsets}] [{lengths}]
     *   example: 127.0.0.1 6667 res/seed.txt 0,0,0,0,0  10,80,50,6,80
     */
    public static void main(String[] args) {
        String IP;
        int port;
        String seedPath;
        if (args.length == 3 || args.length == 5) {
            IP = args[0];
            port = Integer.parseInt(args[1]);
            seedPath = args[2];
            if (args.length == 5) {
                String[] strOffsets = args[3].split(",");
                String[] strLengths = args[4].split(",");
                if (strOffsets.length != 5 || strLengths.length != 5) {
                    logger.error("Wrong offsets or lengths");
                    return;
                }
                for (int i = 0; i < 5; i++) {
                    Config.OFFSETS[i] = Integer.parseInt(strOffsets[i]);
                    Config.LENGTHS[i] = Integer.parseInt(strLengths[i]);
                }
            }
        } else {
            help();
            return;
        }

        ImportCassandra importCassandra;
        try {
            importCassandra = new ImportCassandra(IP, port, seedPath);
        } catch (ImportException e) {
            logger.error("Cannot create importer.", e);
            return;
        }
        importCassandra.doImport();
    }
}
