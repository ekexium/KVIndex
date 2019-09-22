import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HashFuncTest {

    @Test
    void hashFuncCorrectness() {
        Random random = new Random(System.currentTimeMillis());
        int N = random.nextInt(1000000);
        N = 12;
        HashFunc hasher = new HashFunc(N);
        for (int i = 0; i < N; i++) {
            byte[] arr = new byte[random.nextInt(4096)];
            random.nextBytes(arr);
            assertEquals(hasher.hash(arr), hasher.hash(arr));
        }
    }
}
