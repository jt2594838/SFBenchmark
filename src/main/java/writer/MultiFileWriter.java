package writer;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MultiFileWriter {

    private String outputFilePath;
    private int maxEntryPerFile;

    private int currFileIndex;
    private int writtenCnt;
    private CompactFileWriter writer;

    public MultiFileWriter(String outputFilePath, int maxEntryPerFile) throws IOException {
        if (outputFilePath == null) {
            throw new IOException("OutputFilePath cannot be null");
        }
        if (maxEntryPerFile <= 0) {
            throw new IOException("MaxEntryPerFile must be positive");
        }
        this.outputFilePath = outputFilePath;
        this.maxEntryPerFile = maxEntryPerFile;
        openWriter();
    }

    private void openWriter() throws FileNotFoundException {
        FileOutputStream fileOutputStream = null;
        fileOutputStream = new FileOutputStream(outputFilePath + "_" + currFileIndex);
        BufferedOutputStream outputStream = new BufferedOutputStream(fileOutputStream);
        writer = new CompactFileWriter(outputStream);
    }

    public void close() throws IOException {
        writer.close();
    }

    public void write(String path, byte[] contents) throws IOException, IOException {
        if (writtenCnt >= maxEntryPerFile) {
            writtenCnt = 0;
            currFileIndex ++;
            writer.close();
            openWriter();
        }
        writer.write(path, contents);
        writtenCnt ++;
    }
}
