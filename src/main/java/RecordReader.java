import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

class InvalidDataFormatException extends Exception {
    InvalidDataFormatException() {
        super();
    }

    InvalidDataFormatException(String message) {
        super(message);
    }
}

class RecordReader {

    private FileInputStream inputStream;
    private boolean closed = false;
    long pos = 0;

    RecordReader(String filename)
            throws FileNotFoundException {
        inputStream = new FileInputStream(filename);
    }

    RecordReader(String filename, int keySizeLength, int valueSizeLength)
            throws FileNotFoundException {
        this(filename);
        Record.keySizeLength = keySizeLength;
        Record.valueSizeLength = valueSizeLength;
    }

    boolean hasNextRecord() throws IOException {
        if (closed)
            return false;
        return inputStream.available() > 0;
    }

    Record getNextRecord(boolean needData)
            throws IOException, InvalidDataFormatException, BufferUnderflowException {
        Record record = new Record();
        record.address = pos;

        // read key_size
        byte[] keySizeArray = new byte[Record.keySizeLength];
        readArray(inputStream, keySizeArray);
        record.keySize = ByteBuffer.wrap(keySizeArray).order(ByteOrder.BIG_ENDIAN).getShort();
        if (record.keySize < 0 || record.keySize > Record.MAX_KEY_SIZE)
            throw new InvalidDataFormatException("Invalid key size: " + record.keySize);
        pos += Record.keySizeLength;

        // read key
        if (needData) {
            record.key = new byte[record.keySize];
            readArray(inputStream, record.key);
        } else {
            inputStream.skip(record.keySize);
        }
        pos += record.keySize;


        // read value size
        byte[] valueSizeArray = new byte[Record.valueSizeLength];
        readArray(inputStream, valueSizeArray);
        record.valueSize = ByteBuffer.wrap(valueSizeArray).order(ByteOrder.BIG_ENDIAN).getShort();
        if (record.valueSize < 0 || record.valueSize > Record.MAX_VALUE_SIZE)
            throw new InvalidDataFormatException("Invalid value size: " + record.valueSize);
        pos += Record.valueSizeLength;

        // read value
        if (needData) {
            record.value = new byte[record.valueSize];
            readArray(inputStream, record.value);
        } else {
            inputStream.skip(record.valueSize);
        }
        pos += record.valueSize;


        // if reaches EOF, close file
        if (inputStream.available() == 0) {
            inputStream.close();
            closed = true;
        }

        Log.logd("-------reader------");
        Log.logd("k = " + Arrays.toString(record.key));
        Log.logd("v = " + Arrays.toString(record.value));
        Log.logd("addr = " + record.address);
        Log.logd("------\\reader------");


        return record;
    }


    private void readArray(FileInputStream inputStream, byte[] arr)
            throws InvalidDataFormatException, IOException {
        int read = inputStream.read(arr);
        // read = -1 if it reaches the end of the file
        if (read < arr.length)
            throw new InvalidDataFormatException("End of file: no enough data to read, read = " + read);
    }

    void close() throws IOException {
        inputStream.close();
    }
}
