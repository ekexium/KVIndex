import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * A Reader that reads the data file, with record format (key_size, key, value_size, value).
 * Each getNextRecord() call returns a record with {key_size, key, value_size, value, address}.
 */
class RecordReader {

    private FileInputStream inputStream;
    private boolean closed = false;
    long pos = 0;

    /**
     * Constructs the input stream which reads the input file.
     *
     * @param filename
     *        The filename of the data.
     *
     * @throws FileNotFoundException
     *         If data file is not found.
     */
    RecordReader(String filename)
            throws FileNotFoundException {
        inputStream = new FileInputStream(filename);
    }

    /**
     * Constructs the input stream and specify length of key_size and value_size fields.
     *
     * @param filename
     *        The filename of the data.
     * @param keySizeLength
     *        The length of the field key_size.
     * @param valueSizeLength
     *        The length of the field value_size.
     *
     * @throws FileNotFoundException
     *         If data file is not found.
     */
    RecordReader(String filename, int keySizeLength, int valueSizeLength)
            throws FileNotFoundException {
        this(filename);
        Record.keySizeLength = keySizeLength;
        Record.valueSizeLength = valueSizeLength;
    }

    /**
     * Checks if the reader can read next record, or has reached the end of file.
     *
     * @return Whether there is another record that can be read.
     *
     * @throws IOException
     *         If this file input stream has been closed by calling or I/O error occurs.
     */
    boolean hasNextRecord() throws IOException {
        if (closed)
            return false;
        return inputStream.available() > 0;
    }

    /**
     * Reads the next record and returns it.
     *
     * @param needData
     *        Whether need to read key and value
     *
     * @return The read record
     *
     * @throws IOException
     *         If I/O errors occur
     * @throws InvalidDataFormatException
     *         If the data format is invalid, e.g. invalid key_size
     * @throws BufferUnderflowException
     *         If there are fewer bytes than required to get a number from a byte array
     */
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

    /**
     * Read an array of bytes from inputStream
     *
     * @param inputStream
     *        The file input stream
     * @param arr
     *        The destination array
     *
     * @throws InvalidDataFormatException
     *         If read fewer bytes than required
     * @throws IOException
     *         If I/O errors occur
     */
    private void readArray(FileInputStream inputStream, byte[] arr)
            throws InvalidDataFormatException, IOException {
        int read = inputStream.read(arr);
        // read = -1 if it reaches the end of the file
        if (read < arr.length)
            throw new InvalidDataFormatException("End of file: no enough data to read, read = " + read);
    }

    /**
     * close the input stream
     *
     * @throws IOException
     *         If I/O errors occur
     */
    void close() throws IOException {
        inputStream.close();
    }
}

/**
 * The exception class for invalid data format.
 *
 * Examples:
 *      Less data than required in the data file,
 *      invalid key_size,
 *      invalid value_size.
 */
class InvalidDataFormatException extends Exception {
    InvalidDataFormatException() {
        super();
    }

    InvalidDataFormatException(String message) {
        super(message);
    }
}