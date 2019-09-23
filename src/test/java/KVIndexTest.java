import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class KVIndexTest {

    private final KVIndex index = new KVIndex();

    static ArrayList<byte[]> keys = new ArrayList<byte[]>();
    static ArrayList<byte[]> values = new ArrayList<byte[]>();
    static long N;
    static String filename = "data" + File.separator + "data";
    static HashSet<List<Byte>> set = new HashSet<>();

    @BeforeAll
    static void makeData() {
        Random random = new Random(System.currentTimeMillis());
        int n = random.nextInt(200000) + 1;
        makeData(n);
    }

    static void makeData(int n) {
        try {
            keys.clear();
            values.clear();

            Log.logi("Begin generating data.");
            long startTime = System.currentTimeMillis();
            FileOutputStream out = new FileOutputStream(filename);
            long seed = System.currentTimeMillis();
            Log.logi("Seed = " + seed);
            Random random = new Random(seed);

            // generate the number of k-v pairs
            try {
                for (int i = 0; i < n; i++) {
                    Log.logd("[gen] " + i);
                    // generate key size and value size
                    int keySize = random.nextInt(4096) + 1;
                    int valueSize = random.nextInt(4096) + 1;

                    // for benchmark
                    keySize = 4096;
                    valueSize = 4096;

                    // generate key and value
                    byte[] key = new byte[keySize];
                    Byte[] bkey = new Byte[keySize];
                    do {
                        random.nextBytes(key);
                        bkey = ArrayUtils.toObject(key);
                    } while (set.contains(Arrays.asList(bkey)));
                    byte[] value = new byte[valueSize];
                    random.nextBytes(value);
                    set.add(Arrays.asList(bkey));
                    keys.add(key);
                    values.add(value);
                    Log.logd("key = " + Arrays.toString(key));
                    Log.logd("value = " + Arrays.toString(value));

                    // write to file
                    out.write(ByteBuffer.allocate(2).putShort((short) key.length).array());
                    out.write(key);
                    out.write(ByteBuffer.allocate(2).putShort((short) value.length).array());
                    out.write(value);
                }
                Log.logi("Data generated, used "
                         + (System.currentTimeMillis() - startTime) + " " + "ms.");
            } finally {
                out.close();
            }
            N = n;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testCountEntry() {
        String filename = "data" + File.separator + "data";
        try {
            long count = index.countEntry(filename);
            assertEquals(count, N);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    void testCreateIndexFile() {
        long N = 2000;
        index.hasher = new HashFunc(N);
        index.calculateMask();
        index.createIndexFile();
        for (int i = 0; i < (1 << index.f); i++) {
            File file = new File(index.indexPath + File.separator + "index" + i);
            assertTrue(file.exists());
            assertEquals(file.length(), (index.hasher.capacity >> index.f) * index.slotSize);
        }
    }

    @Test
    void testInitialization() {
        try {
            index.initialize(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testCorrectness() {
        try {
            // initialization
            index.initialize(filename);

            // construct queries
            for (int i = 0; i < keys.size(); i++) {
                byte[] value = index.get(keys.get(i));
                if (Arrays.compare(value, values.get(i)) != 0) {
                    Log.logd("value = " + Arrays.toString(value));
                    Log.logd("true value = " + Arrays.toString(values.get(i)));
                }
                assertEquals(Arrays.compare(value, values.get(i)), 0);
            }

            // invalid queries
            Random random = new Random(System.currentTimeMillis());
            for (int i = 0; i < 10; ) {
                byte[] nkey = new byte[Record.MAX_KEY_SIZE];
                for (byte[] key : keys) {
                    if (Arrays.compare(key, nkey) == 0) continue;
                }
                i++;
                assertNull(index.get(nkey));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
