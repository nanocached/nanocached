// Java-SDK smoke for the AWS live tests: java Main <write|read> <label> <count>
// Addresses via NANOTEST_ADDRESSES ("host:port,host:port"). Compile against the SDK sources.
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.nanocached.NanocachedClient;
import org.nanocached.NanocachedClient.Address;

public final class Main {
    public static void main(String[] args) throws Exception {
        List<Address> addresses = new ArrayList<>();
        for (String part : System.getenv("NANOTEST_ADDRESSES").split(",")) {
            int idx = part.lastIndexOf(':');
            addresses.add(new Address(part.substring(0, idx), Integer.parseInt(part.substring(idx + 1))));
        }
        NanocachedClient.Options options = NanocachedClient.builder().addresses(addresses);

        // Checked before connecting: an invalid invocation should fail
        // loudly with a usage message, not crash with an
        // ArrayIndexOutOfBoundsException on a missing argument or (worse)
        // an uncaught NumberFormatException / a non-positive count that
        // silently makes every loop below a no-op, reporting a false
        // "success" as if 0 iterations were intended.
        if (args.length != 3) {
            usage();
        }
        String cmd = args[0];
        String label = args[1];
        int count = parseCount(args[2]);

        int exitCode = 0;

        try (NanocachedClient client = NanocachedClient.connect(options)) {
            if (cmd.equals("write")) {
                for (int i = 0; i < count; i++) {
                    client.set("x:" + label + ":" + i, "v-" + label + "-" + i);
                }
                System.out.println("wrote " + count + " keys for label " + label);
            } else if (cmd.equals("read")) {
                int bad = 0;
                for (int i = 0; i < count; i++) {
                    Optional<String> value = client.get("x:" + label + ":" + i);
                    String expected = "v-" + label + "-" + i;
                    if (value.isEmpty() || !value.get().equals(expected)) {
                        bad++;
                    }
                }
                if (bad > 0) {
                    System.out.println("label " + label + ": " + bad + "/" + count + " BAD");
                    exitCode = 1;
                } else {
                    System.out.println("label " + label + ": " + count + "/" + count + " OK");
                }
            } else {
                System.out.println("unknown command " + cmd);
                exitCode = 2;
            }
        }

        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static void usage() {
        System.err.println("usage: java Main <write|read> <label> <count>");
        System.exit(1);
    }

    private static int parseCount(String raw) {
        try {
            int count = Integer.parseInt(raw);
            if (count <= 0) {
                throw new NumberFormatException("count must be positive");
            }
            return count;
        } catch (NumberFormatException e) {
            System.err.println(
                    "usage: java Main <write|read> <label> <count>: invalid count '" + raw + "'");
            System.exit(1);
            throw new AssertionError("unreachable");
        }
    }
}
