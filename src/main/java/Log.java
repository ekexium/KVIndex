public class Log {
    private static final boolean useDebug = true;
    private static final boolean useWarning = true;

    static void logd(String s) {
        if (useDebug)
            System.out.println(s);
    }

    static void logw(String s) {
        if (useWarning)
            System.out.println(s);
    }
}
