import java.nio.ByteBuffer;

/**
 * A DJB hash function that maps key(bytes[], <= 4096 bytes) to an address(long).
 *
 * The capacity is set to the nearest upper 2^k to (N / preferred_load_factor).
 */
class HashFunc {
    long N;                 // size of the set of keys
    long capacity;          // capacity of slots
    int loadFactorInv = 2;  // the reciprocal of preferred load factor

    static final long MAX_CAPACITY = 0x8000000000L; // 2^40


    /**
     * In the constructor, calculate the capacity.
     *
     * @param N The size of the set of keys
     */
    HashFunc(long N) {
        this.N = N;
        capacity = 1;
        while (capacity <= MAX_CAPACITY && capacity < N)
            capacity <<= 1;

        capacity *= loadFactorInv;
        if (capacity > MAX_CAPACITY || capacity < 0) {
            capacity = MAX_CAPACITY;
            Log.logw("Hash function use MAX_CAPACITY");
        }

        Log.logi("Hash capacity = " + capacity);
    }

    /**
     * DJB hash function.
     * Thread-safe.
     *
     * @param key
     *        Key of hash function
     * @return The hashcode of key
     */
    long hash(byte[] key) {
        long hash = 5381;

        // convert byte[] to long[]
        long[] longArr = new long[(key.length + 7) >> 3];
        int padding = (longArr.length << 3) - key.length;
        ByteBuffer buf = ByteBuffer.allocate(padding + key.length)
                                   .put(new byte[padding])
                                   .put(key);
        buf.flip();
        buf.asLongBuffer().get(longArr);

        // calculate hash code
        for (long l : longArr) {
            hash = (hash << 5) + hash + l;
        }

        // return the lowest k bits as hash code
        return hash & (capacity - 1);
    }
}
