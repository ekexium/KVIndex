import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class KVIndex {
    /**
     * hash() : key -> hashCode
     * hashCode: h bits
     * First (h - f) bits are used for in-file index.
     * Last f bits are used for file index, i.e. there are 2^f index files.
     * <p>
     * Example:
     * h = 24, f = 8
     * fileIdMask:      0x000000ff
     * infileIndexMask: 0x00ffff00
     */
    final int f = 8;
    long fileIdMask;
    long infileIndexMask;

    /**
     * size of each slot in index files
     * next_index_id use 4 bytes, since h - f = 40 - 8 = 32 bits
     * i.e. there should be at most 2^31 -1 indexes in the file
     * key_size | key_pos | next_index_id
     * 2        |    5    |      4
     */

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

    long N; // number of key-value pairs

    long collisionCount = 0;

    HashFunc hasher;

    public static void main(String[] args) {
        System.out.println("Hello PingCAP");
    }

    KVIndex() {
        System.out.println("Hello PingCAP");
    }

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
                        Log.loge("seek offset < 0");
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
                            Log.logd("[get] value size = " + valueSize);
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

                    if (infileIndex < 0)
                        Log.loge("infileIndex < 0");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

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
     * Create empty index files.
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

    private void createIndex(String filename) {
        try {
            RecordReader reader = new RecordReader(filename);
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
                    collisionCount++;

                    indexFile.skipBytes(addrLength + Record.valueSizeLength);
                    byte[] nextPos = new byte[infilePointerLength];
                    indexFile.read(nextPos);

                    // set the pointer to the next slot to the end, where new record is written
                    indexFile.seek(slotSize * infileIndex
                                   + Record.keySizeLength + addrLength + Record.valueSizeLength);
                    indexFile.writeInt((int) (indexFile.length() / slotSize));

                    // append the file
                    indexFile.seek(indexFile.length());
                    writeSlot(indexFile, record, nextPos);
                }
                indexFile.close();
            }
        } catch (IOException | InvalidDataFormatException e) {
            Log.loge("Failed to create index: " + e.getMessage());
            e.printStackTrace();
        }
    }

    void calculateMask() {
        fileIdMask = (1 << f) - 1;
        infileIndexMask = (hasher.capacity - 1) ^ fileIdMask;
    }

    private String getIndexFilePath(int fileId) {
        return indexPath + File.separator + indexFilenamePrefix + fileId + indexFilenamePostfix;
    }

    private void writeSlot(RandomAccessFile indexFile, Record record, byte[] nextPos)
            throws IOException {
        if (nextPos.length != infilePointerLength)
            throw new IllegalArgumentException("Length of nextPos must be 4");

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
        indexFile.write(nextPos);
        Log.logd("-------\\writeslot--------");
    }
}

class UninitializedException extends Exception {
    UninitializedException() {
        super();
    }

    UninitializedException(String msg) {
        super(msg);
    }
}
