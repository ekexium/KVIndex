import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;


class InvalidDataFormatException extends Exception {
    InvalidDataFormatException() {
        super();
    }

    InvalidDataFormatException(String message) {
        super(message);
    }
}

class RecordReader {

    int keySizeLength = 2;         // length of the field key_size, by default 2 bytes
    int valueSizeLength = 2;       // length of the field value_size, by default 2 bytes
    int MAX_KEY_SIZE = 4096;       // 4KB
    int MAX_VALUE_SIZE = 4096;     // 4KB

    private FileInputStream inputStream;
    private boolean closed = false;

    RecordReader(String filename)
            throws FileNotFoundException {
        inputStream = new FileInputStream(filename);
    }

    RecordReader(String filename, int keySizeLength, int valueSizeLength)
            throws FileNotFoundException {
        this(filename);
        this.keySizeLength = keySizeLength;
        this.valueSizeLength = valueSizeLength;
    }

    boolean hasNextRecord() throws IOException {
        if (closed)
            return false;
        return inputStream.available() > 0;
    }

    Record getNextRecord(boolean needData)
            throws IOException, InvalidDataFormatException, BufferUnderflowException {
        // read key_size
        Record record = new Record();
        byte[] keySizeArray = new byte[keySizeLength];
        readArray(inputStream, keySizeArray);
        record.keySize = ByteBuffer.wrap(keySizeArray).order(ByteOrder.BIG_ENDIAN).getShort();
        Log.logd("key_size = " + Integer.toBinaryString((int)keySizeArray[0])
                 + " " + Integer.toBinaryString((int)keySizeArray[1])
                 + " => " + record.keySize);
        if (record.keySize < 0 || record.keySize > MAX_KEY_SIZE)
            throw new InvalidDataFormatException("Invalid key size: " + record.keySize);

        // read key
        if (needData) {
            record.key = new byte[record.keySize];
            readArray(inputStream, record.key);
        } else {
            inputStream.skip(record.keySize);
        }

        // read value size
        byte[] valueSizeArray = new byte[valueSizeLength];
        readArray(inputStream, valueSizeArray);
        record.valueSize = ByteBuffer.wrap(valueSizeArray).order(ByteOrder.BIG_ENDIAN).getShort();
        Log.logd("value_size = " + Integer.toBinaryString((int)valueSizeArray[0])
                 + " " + Integer.toBinaryString((int)valueSizeArray[1])
                 + " => " + record.valueSize);
        if (record.valueSize < 0 || record.valueSize > MAX_VALUE_SIZE)
            throw new InvalidDataFormatException("Invalid value size: " + record.valueSize);

        // read value
        if (needData) {
            record.value = new byte[record.valueSize];
            readArray(inputStream, record.value);
        } else {
            inputStream.skip(record.valueSize);
        }

        // if reaches EOF, close file
        if (inputStream.available() == 0) {
            inputStream.close();
            closed = true;
        }

        return record;
    }


    private void readArray(FileInputStream inputStream, byte[] arr)
            throws InvalidDataFormatException, IOException {
        int read = inputStream.read(arr);
        // read = -1 if it reaches the end of the file
        if (read < keySizeLength)
            throw new InvalidDataFormatException("End of file: no enough data to read");
    }

    void close() throws IOException {
        inputStream.close();
    }
}
