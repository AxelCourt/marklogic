package fr.askjadev.xml.marklogic.query;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory.BasicAuthContext;
import com.marklogic.client.admin.TransformExtensionsManager;
import com.marklogic.client.datamovement.ApplyTransformListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import java.nio.file.Paths;

public class SimpleXQueryTransformation {
    
    // MarkLogic Server connection information
    private final String HOST;
    private final int PORT;
    private final String USER;
    private final String PASSWORD;
    private final String DOC_DIR;
    private final String TRANSF_FILEPATH;
    private DatabaseClient client;
    private ServerTransform txform;
    
    public SimpleXQueryTransformation(String host, String port, String user, String password, String docDir, String transfFilePath) {
        super();
        this.HOST = host;
        this.PORT = Integer.parseInt(port);
        this.USER = user;
        this.PASSWORD = password;
        this.DOC_DIR = docDir;
        this.TRANSF_FILEPATH = transfFilePath;
    }
    
    // Connection
    private void connect() {
        this.client = DatabaseClientFactory.newClient(HOST, PORT, new BasicAuthContext(USER, PASSWORD));
    }
    
    // Register the XQuery transformation
    private void registerTransformation() {
        FileHandle txImpl = new FileHandle().with(
            Paths.get(TRANSF_FILEPATH).toFile()
        );
        TransformExtensionsManager txmgr = client.newServerConfigManager().newTransformExtensionsManager();
        txmgr.writeXQueryTransform("xquery", txImpl);
        this.txform = new ServerTransform("xquery");
    }
    
    // Query
    private void query() {
        // Query the directory for documents
        QueryManager qm = client.newQueryManager();
        StructuredQueryBuilder sqb = qm.newStructuredQueryBuilder();
        StructuredQueryDefinition docQuery = sqb.directory(true,DOC_DIR);
        // Create and configure the batcher
        DataMovementManager dmm = client.newDataMovementManager();
        QueryBatcher batcher = dmm.newQueryBatcher(docQuery);
        batcher
            .onUrisReady(
                new ApplyTransformListener().withTransform(txform)
            )
            .onQueryFailure( exception -> exception.printStackTrace() );
        
        // Start the job
        dmm.startJob(batcher);
        
        // Wait for the job to complete, and then stop it.
        batcher.awaitCompletion();
        dmm.stopJob(batcher);
    
    }

    // Main
    public static void main(String[] args) {
        SimpleXQueryTransformation eltQuery = new SimpleXQueryTransformation(args[0], args[1], args[2], args[3], args[4], args[5]);
        eltQuery.connect();
        eltQuery.registerTransformation();
        eltQuery.query();
    }
    
}
