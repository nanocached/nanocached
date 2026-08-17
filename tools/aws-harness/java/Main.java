// Java-SDK smoke for the AWS live tests: java Main <write|read> <label> <count>
// Seeds via NANOTEST_SEEDS ("host:port,host:port"). Compile against the SDK sources.
import org.nanocached.NanocachedClient;

public final class Main {
    public static void main(String[] args) throws Exception {
        NanocachedClient.Options options = NanocachedClient.builder();
        for (String part : System.getenv("NANOTEST_SEEDS").split(",")) {
            int idx = part.lastIndexOf(':');
            options = options.host(part.substring(0, idx), Integer.parseInt(part.substring(idx + 1)));
        }

        String cmd = args[0];
        String label = args[1];
        int count = Integer.parseInt(args[2]);

        try (NanocachedClient client = NanocachedClient.connect(options)) {
            if (cmd.equals("write")) {
                for (int i = 0; i < count; i++) {
                    client.set("x:" + label + ":" + i, "v-" + label + "-" + i);
                }
                System.out.println("wrote " + count + " keys for label " + label);
            } else if (cmd.equals("read")) {
                int bad = 0;
                for (int i = 0; i < count; i++) {
                    byte[] value = client.get("x:" + label + ":" + i);
                    String expected = "v-" + label + "-" + i;
                    if (value == null || !new String(value).equals(expected)) {
                        bad++;
                    }
                }
                if (bad > 0) {
                    System.out.println("label " + label + ": " + bad + "/" + count + " BAD");
                    System.exit(1);
                }
                System.out.println("label " + label + ": " + count + "/" + count + " OK");
            } else {
                System.out.println("unknown command " + cmd);
                System.exit(2);
            }
        }
    }
}
