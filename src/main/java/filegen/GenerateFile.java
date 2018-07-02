package filegen;

import config.Config;

import java.io.*;
import java.util.Random;

public class GenerateFile {

    public static final char LEVEL_SEPARATOR = '/';
    private final int FILE_NUM = 20000000;
    private final int CHAR_NUM = 1000000;
    private final String ROOT = "root";
    private final String SUFFIX = ".txt";
    private final String SEPARATOR = "/";

    private final int PATTERN_LEN = 5;
    private final String PHYSICS_PREFIX = "physics";
    private final int HEIGHT_SHIFT = 50;
    private final int DATE_START = 10000000;
    private final int TIME_SHIFT = 3;

    //    private int[] LEVEL_NUM = {10,80,50,6,80};
    private int[] LEVEL_NUM;
    private int[] OFFSETS;
    private int[] LENGTHS;

    private String[][] subPathNames;

    private int[] index = OFFSETS;

    private byte[] contentSeed;

    private String currPath;

    public GenerateFile(String seedPath) throws IOException {
        init(seedPath);
    }

    private void init(String seedPath) throws IOException {
        LEVEL_NUM = Config.LEVEL_NUM;
        OFFSETS = Config.OFFSETS;
        LENGTHS = Config.LENGTHS;

        subPathNames = new String[LEVEL_NUM.length][];
        for (int i = 0; i < subPathNames.length; i++) {
            subPathNames[i] = new String[LEVEL_NUM[i]];
        }

        for (int index = 0; index < subPathNames.length; index++) {
            for (int i = 0; i < subPathNames[index].length; i++) {
                if (index == 0) {
                    subPathNames[index][i] = "";
                    for (int j = 0; j < PATTERN_LEN; j++) {
                        subPathNames[index][i] += (char) ('a' + i);
                    }
                }

                if (index == 1) {
                    subPathNames[index][i] = PHYSICS_PREFIX + i;
                }

                if (index == 2) {
                    subPathNames[index][i] = String.valueOf(HEIGHT_SHIFT * i);
                }

                if (index == 3) {
                    subPathNames[index][i] = String.valueOf(DATE_START + i);
                }

                if (index == 4) {
                    subPathNames[index][i] = String.valueOf(TIME_SHIFT * (i + 1));
                }
            }
        }
        readSeed(seedPath);
    }

    private void readSeed(String seedPath) throws IOException {
        File file = new File(seedPath);
        if(!file.isFile()) {
            throw new IOException(String.format("%s not exist", seedPath));
        }
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));
        try {
            contentSeed = new byte[inputStream.available()];
            int lenRead = inputStream.read(contentSeed);
            if (lenRead < contentSeed.length)
                throw new IOException(String.format("Read %d bytes while the file %s has %d bytes", lenRead, seedPath, contentSeed.length));
        } finally {
            inputStream.close();
        }
    }

    private void nextIndex() {
        index[index.length - 1] ++;
        for (int i = index.length - 1; i > 0; i--) {
            if(index[i] - OFFSETS[i] >= LENGTHS[i]) {
                index[i] = 0;
                index[i - 1]++;
            }
        }
    }

    public boolean hasNext() {
        return index[0] - OFFSETS[0] < LENGTHS[0];
    }

    public void next() {
        currPath = getPath(index);
        nextIndex();
    }

    public String getPath(int[] ind) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < LEVEL_NUM.length - 1; i++) {
            stringBuilder.append(subPathNames[i][ind[i]]).append(LEVEL_SEPARATOR);
        }
        stringBuilder.append(subPathNames[LEVEL_NUM.length - 1][ind[LEVEL_NUM.length - 1]]);
        return stringBuilder.toString();
    }

    public byte[] getContentSeed() {
        return contentSeed;
    }

    public String getCurrPath() {
        return currPath;
    }
}
