package it.unimib.disco.bigtwine.streamprocessor;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class GridFSOutputFormat implements OutputFormat<String> {
    private static final Logger LOG = LoggerFactory.getLogger(GridFSOutputFormat.class);
    private static final int EOL = "\n".charAt(0);

    private String gridFsConnectionUri;
    private String gridFsDbName;
    private String analysisId;
    private String documentId;
    private JobHeartbeatSender heartbeatSender;

    private long writtenRecords = 0;
    private long totalRecords;

    private transient MongoClient mongoClient;
    private transient GridFSBucket bucket;
    private transient GridFSUploadStream uploadStream;

    public GridFSOutputFormat() {
    }

    public GridFSOutputFormat(
            String gridFsConnectionUri,
            String gridFsDbName,
            String documentId,
            String analysisId,
            long totalRecords) {
        this(gridFsConnectionUri, gridFsDbName, documentId, analysisId, totalRecords, null);
    }

    public GridFSOutputFormat(
            String gridFsConnectionUri,
            String gridFsDbName,
            String documentId,
            String analysisId,
            long totalRecords,
            JobHeartbeatSender heartbeatSender) {
        this.gridFsConnectionUri = gridFsConnectionUri;
        this.gridFsDbName = gridFsDbName;
        this.analysisId = analysisId;
        this.documentId = documentId;
        this.totalRecords = totalRecords;
        this.heartbeatSender = heartbeatSender;
    }

    public String getGridFsConnectionUri() {
        return gridFsConnectionUri;
    }

    public void setGridFsConnectionUri(String gridFsConnectionUri) {
        this.gridFsConnectionUri = gridFsConnectionUri;
    }

    public String getGridFsDbName() {
        return gridFsDbName;
    }

    public void setGridFsDbName(String gridFsDbName) {
        this.gridFsDbName = gridFsDbName;
    }

    public String getAnalysisId() {
        return analysisId;
    }

    public void setAnalysisId(String analysisId) {
        this.analysisId = analysisId;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public JobHeartbeatSender getHeartbeatSender() {
        return heartbeatSender;
    }

    public void setHeartbeatSender(JobHeartbeatSender heartbeatSender) {
        this.heartbeatSender = heartbeatSender;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public long getWrittenRecords() {
        return writtenRecords;
    }

    public void setWrittenRecords(long writtenRecords) {
        this.writtenRecords = writtenRecords;
    }

    @Override
    public void configure(Configuration parameters) {
        if (this.mongoClient == null) {
            this.mongoClient = MongoClients.create(gridFsConnectionUri);
        }

        if (this.bucket == null) {
            this.bucket = GridFSBuckets.create(mongoClient.getDatabase(gridFsDbName));
        }
    }

    @Override
    public void open(int taskNumber, int numTasks) {
        Document metadata = new Document();
        metadata.put("analysis-id", analysisId);

        GridFSUploadOptions options = new GridFSUploadOptions()
                .metadata(metadata);

        this.uploadStream = this.bucket.openUploadStream(
                new BsonObjectId(new ObjectId(this.documentId)),
                "output-" + analysisId + ".tsv",
                options
        );
    }

    @Override
    public void writeRecord(String record) {
        byte[] bytes = record.getBytes();
        this.uploadStream.write(bytes);
        this.uploadStream.write(EOL);

        this.writtenRecords++;
        LOG.debug("Written records {}", this.writtenRecords);

        if (this.heartbeatSender != null) {
            double progress = this.getProgress();
            this.heartbeatSender.send(progress, progress == 1.0);
        }
    }

    @Override
    public void close() {
        this.uploadStream.close();
    }

    public double getProgress() {
        return totalRecords > 0 ? writtenRecords / (double) totalRecords : 0;
    }
}
