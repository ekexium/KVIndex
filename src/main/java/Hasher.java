class HashFunc {
    /**
     *  A hash function that maps key(bytes[], <= 4096 bytes) to an address(<= 8 bytes).
     *
     *  The capacity is set to the nearest 2^k to (N / preferred_load_factor).
     */

    long N;                 // size of the set of keys
    long capacity;          // capacity of slots
    int loadFactorInv = 2;  // the reciprocal of preferred load factor

    HashFunc(long N) {
        this.N = N;
    }

    long hash(byte[] key) {
        // TODO
        return 0;
    }
}
