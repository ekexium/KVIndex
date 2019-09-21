import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KVIndexTest {

    private final KVIndex index = new KVIndex();

    private long makeData(String filename) throws IOException {
        FileOutputStream out = new FileOutputStream(filename);
        Random random = new Random(System.currentTimeMillis());

        // generate the number of k-v pairs
        int n = random.nextInt(10000) + 1;

        try {
            for (int i = 0; i < n; i++) {
                // generate key size and value size
                int keySize = random.nextInt(4096) + 1;
                int valueSize = random.nextInt(4096) + 1;

                // generate key and value
                byte[] key = ByteBuffer.allocate(keySize).array();
                random.nextBytes(key);
                byte[] value = ByteBuffer.allocate(valueSize).array();
                random.nextBytes(value);

                // write to file
                out.write(ByteBuffer.allocate(2).putShort((short)key.length).array());
                out.write(key);
                out.write(ByteBuffer.allocate(2).putShort((short)value.length).array());
                out.write(value);
            }
        } finally {
            out.close();
        }
        return n;
    }

    @Test
    void testCountEntry() {
        String filename = "data";
        try {
            long n = makeData(filename);
            long count = index.countEntry(filename);
            assertEquals(count, n);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
