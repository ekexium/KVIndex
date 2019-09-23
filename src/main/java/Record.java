/**
 * The record class that stores info of a record and some metadata about records.
 *
 * By default, assume key size and value size <= 4KB
 */
public class Record {


    static int keySizeLength = 2;         // length of the field key_size, by default 2 bytes
    static int valueSizeLength = 2;       // length of the field value_size, by default 2 bytes
    static int MAX_KEY_SIZE = 4096;       // 4KB
    static int MAX_VALUE_SIZE = 4096;     // 4KB

    long address;       // address in the original data file

    short keySize;
    short valueSize;
    byte[] key;
    byte[] value;
}
