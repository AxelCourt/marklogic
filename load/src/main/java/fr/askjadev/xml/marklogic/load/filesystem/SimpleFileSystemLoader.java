package fr.askjadev.xml.marklogic.load.filesystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.marklogic.client.io.*;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory.BasicAuthContext;

public class SimpleFileSystemLoader {

    // MarkLogic Server connection information
    private final String HOST;
    private final int PORT;
    private final String USER;
    private final String PASSWORD;
    private final String DATA_DIR;
    private final String DEST_DIR;
    private final String DEST_COLL;
    private DatabaseClient client;
    
    public SimpleFileSystemLoader(String host, String port, String user, String password, String dataDir, String destDir, String destColl) {
        super();
        this.HOST = host;
        this.PORT = Integer.parseInt(port);
        this.USER = user;
        this.PASSWORD = password;
        this.DATA_DIR = dataDir;
        this.DEST_DIR = destDir;
        this.DEST_COLL = destColl;
    }

    // Connection
    private void connect() {
        this.client = DatabaseClientFactory.newClient(HOST, PORT, new BasicAuthContext(USER, PASSWORD));
    }

    // Loading files into the database asynchronously
    private void importDocs() {
        // Create and configure the job
        DataMovementManager dmm = client.newDataMovementManager();
        WriteBatcher batcher = dmm.newWriteBatcher();
        batcher
            .withBatchSize(1000).withThreadCount(5).onBatchSuccess(
                batch -> {
                    System.out.println(
                        batch.getTimestamp().getTime()
                        + " documents written: "
                        + batch.getJobWritesSoFar()
                    );
                }
            )
            .onBatchFailure(
                (batch, throwable) -> {
                    throwable.printStackTrace();
                }
            );

        // Start the job and feed input to the batcher
        dmm.startJob(batcher);
        try {
            Files.walk(Paths.get(DATA_DIR)).filter(Files::isRegularFile).forEach(
                p -> {
                    String uri = DEST_DIR + p.getFileName().toString();
                    FileHandle handle = new FileHandle().with(p.toFile());
                    DocumentMetadataHandle defaultMetadata = new DocumentMetadataHandle().withCollections(DEST_COLL);
                    batcher.add(uri, defaultMetadata, handle);
                }
            );
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Start any partial batches waiting for more input, then wait
        // for all batches to complete. This call will block.
        batcher.flushAndWait();
        dmm.stopJob(batcher);
    }

    // Main
    public static void main(String[] args) {
        SimpleFileSystemLoader fsLoader = new SimpleFileSystemLoader(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        fsLoader.connect();
        fsLoader.importDocs();
    }

}
