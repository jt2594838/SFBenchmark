package filegen;

import exception.GeneratorException;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * This class generates the paths and contents of small files.
 */
public class FileGenerator {
    public static final char LEVEL_SEPARATOR = '/';

    private String[] levelPrefix;
    private int[] levelInstanceNums;
    private int fileSizeInByte;

    private int[] currIndex;
    private int levelNum;
    private String currPath;
    private byte[] currContent;

    public FileGenerator(String[] levelPrefixes, int[] levelInstanceNums,
                         int fileSizeInByte) throws GeneratorException {
        if (levelPrefixes == null) {
            throw new GeneratorException("Level prefixes cannot be null");
        } else if (levelInstanceNums == null) {
            throw new GeneratorException("Level instance numbers cannot be null");
        }
        if (levelPrefixes.length != levelInstanceNums.length){
            throw new GeneratorException(String.format("Got %d level prefixes while %d level instance numbers",
                    levelPrefixes.length, levelInstanceNums.length));
        }
        if (levelPrefixes.length <= 0) {
            throw new GeneratorException("Empty prefixes");
        }
        if (fileSizeInByte <= 1) {
            throw new GeneratorException("File size must be at least 2 bytes");
        }
        for (int i = 0; i < levelPrefixes.length; i++) {
            if (levelPrefixes[i] == null || "".equals(levelPrefixes[i])) {
                throw new GeneratorException(String.format("LevelPrefixes[%d] is empty or null", i));
            }
            if (levelInstanceNums[i] <= 0) {
                throw new GeneratorException(String.format("LevelInstanceNums[%d] is not positive", i));
            }
        }

        this.levelPrefix = levelPrefixes;
        this.levelInstanceNums = levelInstanceNums;

        this.fileSizeInByte = fileSizeInByte;
        this.levelNum = levelPrefixes.length;
        this.currIndex = new int[this.levelNum];
    }

    public void updateCurrPath() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < levelNum - 1; i++) {
            stringBuilder.append(levelPrefix[i]);
            stringBuilder.append(currIndex[i]);
            stringBuilder.append(LEVEL_SEPARATOR);
        }
        stringBuilder.append(levelPrefix[levelNum - 1]);
        stringBuilder.append(currIndex[levelNum - 1]);

        currPath = stringBuilder.toString();
    }

    public String getCurrPath() {
        return currPath;
    }

    public void updateCurrContent() {
        String strContent = RandomStringUtils.randomAlphanumeric(fileSizeInByte / Character.BYTES);
        currContent = strContent.getBytes();
    }

    public byte[] getCurrContent() {
        return currContent;
    }

    public boolean hasNext() {
        return currIndex[0] < levelInstanceNums[0];
    }

    private void nextIndex() {
        currIndex[levelNum - 1] += 1;
        for (int i = levelNum - 1; i > 0; i--) {
            if (currIndex[i] >= levelInstanceNums[i]) {
                    currIndex[i] = 0;
                    currIndex[i - 1] += 1;
            }
        }
    }

    public void next() {
        updateCurrPath();
        updateCurrContent();
        nextIndex();
    }
}
