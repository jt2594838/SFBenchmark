package writer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class CompactFileWriter {

    private OutputStream output;
    private ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

    public CompactFileWriter(OutputStream output) {
        this.output = output;
    }

    public void write(String path, byte[] contents) throws IOException {
        byte[] pathBytes = path.getBytes();
        lengthBuffer.putInt(pathBytes.length);
        output.write(lengthBuffer.array());
        output.write(pathBytes);

        lengthBuffer.clear();
        lengthBuffer.putInt(contents.length);
        output.write(lengthBuffer.array());
        output.write(contents);
    }

    public void close() throws IOException {
        output.close();
    }
}
