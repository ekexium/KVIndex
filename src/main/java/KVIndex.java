import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class KVIndex {
    /*
        hash() : key -> hashCode
        hashCode: h bits
        First (h - f) bits are used for in-file index.
        Last f bits are used for file index, i.e. there are 2^f index files.
     */
    int f;
    int h;
    long fileIdMask;
    long infileIndexMask;

    long N; // number of key-value pairs

    HashFunc hasher;

    public static void main(String[] args) {
        System.out.println("Hello PingCAP");
    }

    KVIndex() {
        System.out.println("Hello PingCAP");
    }

    public void initialize(String filename)
            throws IOException, InvalidDataFormatException {
        // TODO
        N = countEntry(filename);
        hasher = new HashFunc(N);

        calculateF();
        createIndexFile();
        createIndex(filename);
    }

    synchronized public byte[] get(byte[] key) throws UninitializedException {
        // TODO
        if (hasher == null) {
            throw new UninitializedException("KVIndex has not been initialized");
        }
        long hashCode = hasher.hash(key);
        long fileId = hashCode & fileIdMask;
        long inFileIndex = hashCode & infileIndexMask;

        try {
            RandomAccessFile file = new RandomAccessFile(getIndexFilename(fileId), "r");
        } catch (FileNotFoundException e) {
            System.out.println("File not found for index file " + fileId);
        }
        return null;
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
    private void createIndexFile() {

    }

    private void createIndex(String filename) {

    }

    private void calculateF() {

    }

    private String getIndexFilename(long fileId) {
        //TODO
        return null;
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
