import java.nio.ByteBuffer;

class HashFunc {
    /**
     * A DJB hash function that maps key(bytes[], <= 4096 bytes) to an address(<= 8 bytes).
     * <p>
     * The capacity is set to the nearest 2^k to (N / preferred_load_factor).
     */

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
        if (capacity > MAX_CAPACITY) {
            capacity = MAX_CAPACITY;
            Log.logw("Hash functions use MAX_CAPACITY");
        }
    }

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

        return hash;
    }
}
