public class Log {
    private static final boolean useDebug = false;

    static void logd(String s) {
        if (useDebug)
            System.out.println(s);
    }
}
