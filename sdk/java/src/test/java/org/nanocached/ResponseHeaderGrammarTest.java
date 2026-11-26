package org.nanocached;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A response header line must match the wire grammar exactly: one space after the marker, fields
 * separated by single spaces, a bare {@code '\n'} at the end. Any deviation poisons the connection,
 * as in the Go and .NET SDKs. The header used to be trimmed, so {@code V5\n}, {@code V 5 \n},
 * {@code V 5\r\n} and a leading tab were all accepted.
 */
@Timeout(30)
class ResponseHeaderGrammarTest {

    private static final byte[] KEY = "k".getBytes(StandardCharsets.UTF_8);

    @Test
    void aWellFormedValueHeaderIsAccepted() throws Exception {
        withConnection(false, tag -> "V 5\nhello", connection -> {
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), connection.get(KEY));
        });
    }

    @Test
    void malformedUntaggedValueHeadersPoisonTheConnection() throws Exception {
        for (String header : List.of("V5\n", "V 5 \n", "V 5\r\n", "V\t5\n", "V \t5\n", "V  5\n")) {
            withConnection(false, tag -> header + "hello", connection -> {
                assertThrows(
                        NanocachedException.ConnectionLost.class,
                        () -> connection.get(KEY),
                        "header "
                                + header.replace("\r", "\\r")
                                        .replace("\n", "\\n")
                                        .replace("\t", "\\t"));
                assertTrue(connection.isClosed(), "the connection must be poisoned after " + header);
            });
        }
    }

    @Test
    void aWellFormedTaggedStatusIsAccepted() throws Exception {
        withConnection(true, tag -> "S " + tag + "\n", connection -> connection.set(KEY, KEY, null));
    }

    @Test
    void malformedTaggedStatusHeadersPoisonTheConnection() throws Exception {
        for (String format : List.of("S%s\n", "S %s \n", "S %s\r\n", "S\t%s\n", "S  %s\n")) {
            withConnection(true, tag -> String.format(format, tag), connection -> {
                assertThrows(
                        NanocachedException.ConnectionLost.class,
                        () -> connection.set(KEY, KEY, null),
                        "header "
                                + format.replace("\r", "\\r")
                                        .replace("\n", "\\n")
                                        .replace("\t", "\\t"));
                assertTrue(connection.isClosed(), "the connection must be poisoned after " + format);
            });
        }
    }

    interface Reply {
        String to(String tag);
    }

    interface Check {
        void run(Connection connection) throws Exception;
    }

    /**
     * Opens a loopback {@link Connection} whose peer reads one request header line and answers it
     * with {@code reply} (given the request's tag, the header's last field, on a tagged connection).
     */
    private static void withConnection(boolean tagged, Reply reply, Check check) throws Exception {
        try (ServerSocket server = new ServerSocket(0);
                Socket clientSocket = new Socket("127.0.0.1", server.getLocalPort());
                Socket serverSocket = server.accept()) {
            CompletableFuture<Void> peer = CompletableFuture.runAsync(() -> {
                try {
                    String request = readLine(serverSocket.getInputStream());
                    String[] fields = request.split(" ");
                    String tag = tagged ? fields[fields.length - 1] : null;
                    serverSocket.getOutputStream().write(reply.to(tag).getBytes(StandardCharsets.US_ASCII));
                    serverSocket.getOutputStream().flush();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            Connection connection = new Connection(clientSocket, tagged, () -> {});
            try {
                check.run(connection);
            } finally {
                connection.close();
            }
            peer.get(10, TimeUnit.SECONDS);
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        for (int b = in.read(); b != '\n'; b = in.read()) {
            if (b == -1) throw new IOException("closed before the request header ended");
            line.write(b);
        }
        return line.toString(StandardCharsets.US_ASCII);
    }
}
