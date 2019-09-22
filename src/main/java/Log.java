public class Log {
    private static final boolean useDebug = false;
    private static final boolean useWarning = true;
    private static final boolean useInfo = true;
    private static final boolean useError = true;

    static void logd(String s) {
        if (useDebug)
            System.out.println("[Debug] " + s);
    }

    static void logw(String s) {
        if (useWarning)
            System.out.println("[Warning] " + s);
    }

    static void logi(String s) {
        if (useInfo)
            System.out.println("[Info] " + s);
    }

    static void loge(String s) {
        if (useError)
            System.out.println("[Error] " + s);
    }
}
