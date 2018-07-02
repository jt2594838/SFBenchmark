package reader;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class CompactFileReader {

    private DataInputStream inputStream;
    private String currPath;
    private byte[] currContent;

    public CompactFileReader(InputStream inputStream) {
        this.inputStream = new DataInputStream(inputStream);
    }

    public boolean hasNext() throws IOException {
        return inputStream.available() > 0;
    }

    public void next() throws IOException {
        int pathLength = inputStream.readInt();
        byte[] pathBytes = new byte[pathLength];
        inputStream.readFully(pathBytes);
        currPath = new String(pathBytes);

        int contentLength = inputStream.readInt();
        currContent = new byte[contentLength];
        inputStream.readFully(currContent);
    }

    public void close() throws IOException {
        inputStream.close();
    }

    public String getCurrPath() {
        return currPath;
    }

    public byte[] getCurrContent() {
        return currContent;
    }
}
