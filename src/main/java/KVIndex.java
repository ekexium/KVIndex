import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The main class of KVIndex using hash indexing.
 *
 * Input:
 *  A binary data file consisting of records.
 *  Each record is of format (key_size, key, value_size, value).
 *
 * Output:
 *  get(key) returns the corresponding value.
 *
 * Implementation:
 *  During initialization, create index files for all records.
 *  Each files consists of several slots.
 *
 *  Default slot structure:
 *  | key_size | address | value_size | next_slot_id |
 *  |    2     |    5    |      2     |       4      |
 *
 *  address indicates the address of the original record in the data file.
 *
 *  key_size can be used to reduce unnecessary checks.
 *
 *  key_size and value_size can be used to reduce unnecessary disk accesses.
 *
 *  Use linked list to handle collisions.
 *  next_slot_id indicates the id of the next slot in the linked list,
 *  whose address = slot_size(11) * next_slot_id.
 *
 * Indexing:
 *  hash() : key -> hashCode
 *  hashCode: h bits
 *  First (h - f) bits are used for in-file index.
 *  Last f bits are used for file index, i.e. there are 2^f index files.
 *
 *  Example:
 *  h = 24, f = 8
 *  fileIdMask:      0x000000ff
 *  infileIndexMask: 0x00ffff00
 */
public class KVIndex {
    final int f = 8;        // # of bits used for file id
    long fileIdMask;        // bitwise mask for file id
    long infileIndexMask;   // bitwise mask for in-file index

    // constants used to specify the format of index slots
    private static final int addrLength = 5;
    private static final int infilePointerLength = 4;
    static int slotSize = Record.keySizeLength + addrLength
                          + Record.valueSizeLength + infilePointerLength;


    // index file info
    String indexPath = "data" + File.separator + "index";
    final String indexFilenamePrefix = "index";
    final String indexFilenamePostfix = "";

    // original data file
    RandomAccessFile dataFile;

    // number of key-value pairs
    long N;

    // hash function
    HashFunc hasher;

    KVIndex() {
        System.out.println("Hello PingCAP");
    }

    /**
     * Creates index to get ready for queries.
     *
     * @param filename
     *        The filename of data.
     *
     * @throws IOException
     *         If I/O errors occur.
     * @throws InvalidDataFormatException
     *         If the data file has invalid format.
     */
    public void initialize(String filename)
            throws IOException, InvalidDataFormatException {
        N = countEntry(filename);
        Log.logi("N = " + N);
        hasher = new HashFunc(N);
        calculateMask();
        createIndexFile();
        createIndex(filename);
        dataFile = new RandomAccessFile(filename, "r");
    }

    /**
     * Thread-safe query function that returns the value corresponding to the given key.
     *
     * @param key
     *        Key of the query.
     *
     * @return The value.
     *
     * @throws UninitializedException
     *         If the KVIndex object has not been initialized.
     */
    synchronized public byte[] get(byte[] key) throws UninitializedException {
        if (hasher == null)
            throw new UninitializedException("KVIndex has not been initialized");

        // hash
        long hashCode = hasher.hash(key);
        int fileId = (int) (hashCode & fileIdMask);
        long infileIndex = hashCode >>> f;

        // hashcode => index => data
        try {
            try (RandomAccessFile indexFile =
                         new RandomAccessFile(getIndexFilePath(fileId), "r")) {
                byte[] slotArr = new byte[slotSize];

                while (true) {
                    if (slotSize * infileIndex < 0) {
                        Log.logi("seek offset < 0");
                        return null;
                    }
                    indexFile.seek(slotSize * infileIndex);
                    indexFile.read(slotArr);
                    ByteBuffer buf = ByteBuffer.wrap(slotArr);
                    short keySize = buf.getShort(0);

                    // check key size first
                    // key1 == key2 => key1.length == key2.length
                    if (keySize == key.length) {
                        // retrieve key from data file and compare

                        // read key from data file
                        // address(5 bytes) = slotArr[keySizeLength, keySizeLength + addrLength]
                        byte[] addrArr = new byte[8];
                        System.arraycopy(slotArr, Record.keySizeLength,
                                         addrArr, 8 - addrLength, addrLength);
                        long address = ByteBuffer.wrap(addrArr).getLong();
                        dataFile.seek(address + Record.keySizeLength);
                        byte[] keyInData = new byte[keySize];
                        dataFile.read(keyInData);

                        // compare key
                        if (Arrays.equals(key, keyInData)) {
                            // find the key-value
                            // retrieve and return value
                            short valueSize = buf.getShort(Record.keySizeLength + addrLength);
                            byte[] value = new byte[valueSize];
                            dataFile.seek(address + Record.keySizeLength
                                          + keySize + Record.valueSizeLength);
                            dataFile.read(value);
                            return value;
                        }
                    }

                    // key does not match
                    // go to next slot on the chain
                    infileIndex = buf.getInt(Record.keySizeLength
                                             + addrLength + Record.valueSizeLength);

                    if (infileIndex <= 0) {
                        return null;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Counts the total number of records.
     *
     * @param filename
     *        The filename of data.
     *
     * @return The number of records in the data
     *
     * @throws IOException
     *         If I/O errors occur
     * @throws InvalidDataFormatException
     *         If the data file has invalid format.
     */
    long countEntry(String filename) throws IOException, InvalidDataFormatException {
        long rt = 0;
        RecordReader reader = new RecordReader(filename);
        while (reader.hasNextRecord()) {
            Record record = reader.getNextRecord(false);
            rt++;
        }
        return rt;
    }

    /**
     * Creates empty index files.
     */
    void createIndexFile() {
        try {
            new File("data/index").mkdirs();
            for (int i = 0; i < (1 << f); i++) {
                try {
                    File file = new File(getIndexFilePath(i));
                    file.delete();
                    file.createNewFile();
                    FileOutputStream out = new FileOutputStream(file);
                    byte[] emptyArr = new byte[slotSize];
                    for (long j = 0; j < (hasher.capacity >>> f); j++) {
                        out.write(emptyArr);
                    }
                    out.close();
                } catch (IOException e) {
                    Log.loge("Failed to create empty index file " + i + ": " + e.getMessage());
                }
            }
        } catch (SecurityException e) {
            Log.loge("Failed to create empty index file because of security exception: "
                     + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Creates index for every record.
     *
     * @param filename The filename of data
     */
    private void createIndex(String filename) {
        try {
            Log.logi("Begin creating index.");
            long startTime = System.currentTimeMillis();
            RecordReader reader = new RecordReader(filename);

            // open data file to check replicated key

            while (reader.hasNextRecord()) {
                // get a record
                Record record = reader.getNextRecord(true);
                long hashcode = hasher.hash(record.key);
                int fileId = (int) (hashcode & fileIdMask);
                long infileIndex = hashcode >>> f;

                // read the slot from index
                String indexFilename = getIndexFilePath(fileId);
                RandomAccessFile indexFile = new RandomAccessFile(indexFilename, "rw");

                // read key_size
                byte[] keySizeArr = new byte[Record.keySizeLength];
                indexFile.seek(slotSize * infileIndex);
                int read = indexFile.read(keySizeArr);
                short keySize = ByteBuffer.wrap(keySizeArr).getShort();
                if (keySize == 0) {
                    // empty slot, direct write to it
                    indexFile.seek(slotSize * infileIndex);
                    writeSlot(indexFile, record, new byte[infilePointerLength]);
                } else {
                    // hash collision, need to add new slot
                    // temporarily store the address of next slot
                    // skip the key_position and value_size field
                    indexFile.skipBytes(addrLength + Record.valueSizeLength);
                    byte[] nextPos = new byte[infilePointerLength];
                    indexFile.read(nextPos);

                    // set the pointer to the next slot to the end, where new record is written
                    indexFile.seek(slotSize * infileIndex
                                   + Record.keySizeLength + addrLength +
                                   Record.valueSizeLength);
                    indexFile.writeInt((int) (indexFile.length() / slotSize));

                    // append the file
                    indexFile.seek(indexFile.length());
                    writeSlot(indexFile, record, nextPos);
                }
                indexFile.close();
            }
            Log.logi("Index created, used " + (System.currentTimeMillis() - startTime) + "ms.");
        } catch (IOException | InvalidDataFormatException e) {
            Log.loge("Failed to create index: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Calculates the masks for file id and infile index.
     */
    void calculateMask() {
        fileIdMask = (1 << f) - 1;
        infileIndexMask = (hasher.capacity - 1) ^ fileIdMask;
    }

    /**
     * Returns the index file name
     *
     * @param fileId
     *        The id of the index file
     *
     * @return The filename
     */
    private String getIndexFilePath(int fileId) {
        return indexPath + File.separator + indexFilenamePrefix + fileId + indexFilenamePostfix;
    }

    /**
     * Writes a record to the current position of indexFile.
     *
     * @param indexFile
     *        File with position
     * @param record
     *        The record to be written
     * @param nextSlotId
     *        The next slot id in the linked list
     *
     * @throws IOException
     *         If I/O errors occur.
     */
    private void writeSlot(RandomAccessFile indexFile, Record record, byte[] nextSlotId)
            throws IOException {
        if (nextSlotId.length != infilePointerLength)
            throw new IllegalArgumentException("Length of nextSlotId must be 4");

        Log.logd("--------writeslot--------");
        Log.logd("key = " + Arrays.toString(record.key));
        Log.logd("addr = " + record.address);
        Log.logd("value size = " + record.valueSize);
        Log.logd("value = " + Arrays.toString(record.value));


        // key_size
        indexFile.writeShort(record.keySize);

        // position of key in original data file
        // 5 bytes, since the address space of the original data is 1 TB = 2^40 bytes
        byte[] addrArr = Arrays.copyOfRange(ByteBuffer.allocate(8).putLong(record.address).array()
                , 8 - addrLength, 8);
        Log.logd("written address: " + Arrays.toString(addrArr));
        indexFile.write(addrArr);

        // value_size
        indexFile.writeShort(record.valueSize);

        // position of next slot if there is hash collision
        indexFile.write(nextSlotId);
        Log.logd("-------\\writeslot--------");
    }
}

/**
 * The exception class for queries before initialization.
 */
class UninitializedException extends Exception {
    UninitializedException() {
        super();
    }

    UninitializedException(String msg) {
        super(msg);
    }
}
