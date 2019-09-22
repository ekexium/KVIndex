import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KVIndexTest {

    private final KVIndex index = new KVIndex();

    ArrayList<byte[]> keys = new ArrayList<byte[]>();
    ArrayList<byte[]> values = new ArrayList<byte[]>();

    private long makeData(String filename, boolean testCorrectness) throws IOException {
        // when testing correctness, value = [key key]

        FileOutputStream out = new FileOutputStream(filename);
        long seed = System.currentTimeMillis();
        Log.logd("Seed = " + seed);
        Random random = new Random(seed);

        // generate the number of k-v pairs
        int n = random.nextInt(10000) + 1;
        try {
            for (int i = 0; i < n; i++) {
                Log.logd("[gen] " + i);
                // generate key size and value size
                int keySize = random.nextInt(4096) + 1;
                int valueSize = random.nextInt(4096) + 1;

                // generate key and value
                byte[] key = new byte[keySize];
                random.nextBytes(key);
                byte[] value = new byte[valueSize];
                random.nextBytes(value);
                if (testCorrectness) {
                    keys.add(key);
                    values.add(value);
                }
                Log.logd("key = " + Arrays.toString(key));
                Log.logd("value = " + Arrays.toString(value));

                // write to file
                out.write(ByteBuffer.allocate(2).putShort((short) key.length).array());
                out.write(key);
                out.write(ByteBuffer.allocate(2).putShort((short) value.length).array());
                out.write(value);
            }
        } finally {
            out.close();
        }
        return n;
    }

    @Test
    void testCountEntry() {
        String filename = "data" + File.separator + "data";
        try {
            long n = makeData(filename, false);
            long count = index.countEntry(filename);
            assertEquals(count, n);
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
            String filename = "data" + File.separator + "data";
            makeData(filename, false);
            index.initialize(filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testCorrectness() {
        try {
            // initialization
            String filename = "data" + File.separator + "data";
            makeData(filename, true);
            index.initialize(filename);

            // construct queries
            for (int i = 0; i < keys.size(); i++) {
                byte[] value = index.get(keys.get(i));
                Log.logd("value = " + Arrays.toString(value));
                Log.logd("true value = " + Arrays.toString(values.get(i)));
                assertEquals(Arrays.compare(value, values.get(i)), 0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
