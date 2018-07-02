package entity;

import exception.FormatException;
import filegen.FileGenerator;

public class MeteorologicalData {
    public String pattern;
    public String measurement;
    public String layer;
    public String date;
    public String interval;
    public byte[] content;

    public MeteorologicalData(String path, byte[] content) throws FormatException {
        String[] pathSegs = path.split(String.valueOf(FileGenerator.LEVEL_SEPARATOR));
        if (pathSegs.length != 5) {
            throw new FormatException(String.format("Invalid path : %s ", path));
        }

        pattern = pathSegs[0];
        measurement = pathSegs[1];
        layer = pathSegs[2];
        date = pathSegs[3];
        interval = pathSegs[4];
        this.content = content;
    }
}
