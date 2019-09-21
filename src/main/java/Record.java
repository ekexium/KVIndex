public class Record {
    // key size and value size <= 4KB
    short keySize;
    short valueSize;
    byte[] key;
    byte[] value;
}
