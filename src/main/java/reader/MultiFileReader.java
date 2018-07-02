package reader;

import java.io.*;

public class MultiFileReader {

    private File[] inputFiles;
    private int currReaderIndex;
    private CompactFileReader currReader;

    public MultiFileReader(String inputDir) throws IOException {
        File iDir = new File(inputDir);
        if (! iDir.isDirectory()) {
            throw new IOException(String.format("%s is not a directory", inputDir));
        }
        inputFiles = iDir.listFiles();
        if (inputFiles == null || inputFiles.length == 0) {
            throw new IOException(String.format("No file under %s"));
        }
        openReader();
    }

    private void openReader() throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(inputFiles[currReaderIndex]);
        BufferedInputStream inputStream = new BufferedInputStream(fileInputStream);
        currReader = new CompactFileReader(inputStream);
    }

    public boolean hasNext() throws IOException {
        if (currReader.hasNext())
            return true;
        else if (currReaderIndex < inputFiles.length) {
            currReader.close();
            currReaderIndex ++;
            openReader();
            return hasNext();
        }
        return false;
    }

    public void next() throws IOException {
        currReader.next();
    }

    public void close() throws IOException {
        currReader.close();
    }

    public String getCurrPath() {
        return currReader.getCurrPath();
    }

    public byte[] getCurrContent() {
        return currReader.getCurrContent();
    }

}
