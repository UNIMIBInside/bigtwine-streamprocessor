package it.unimib.disco.bigtwine.streamprocessor.source;

import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.types.ObjectId;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GridFSCsvSource implements SourceFunction<Map<String, String>> {
    private final String mongoHost;
    private final int mongoPort;
    private final String dbName;
    private final ObjectId objectId;
    private char fieldDelimiter = '\t';
    private char lineDelimiter = '\n';
    private boolean cancelled = false;

    private transient GridFS fs;

    public GridFSCsvSource(String mongoHost, int mongoPort, String dbName, ObjectId objectId) {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.dbName = dbName;
        this.objectId = objectId;
    }

    private GridFS getFs() {
        if (this.fs == null) {
            MongoClient mongoClient = new MongoClient(mongoHost, mongoPort);
            this.fs = new GridFS(mongoClient.getDB(this.dbName));
        }

        return this.fs;
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        GridFSDBFile file = this.getFs().findOne(this.objectId);
        Reader reader = new InputStreamReader(file.getInputStream());

        CSVParser parser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter(fieldDelimiter)
                .withRecordSeparator(lineDelimiter)
                .parse(reader);
        Iterator<CSVRecord> records = parser.iterator();

        long i = 0;
        while (records.hasNext()) {
            if (cancelled) {
                break;
            }

            CSVRecord record = records.next();
            Map<String, String> recordMap = record.toMap();

            if (recordMap.size() > 0) {
                ctx.collect(recordMap);
            }

            if (i % 10 == 0) {
                ctx.markAsTemporarilyIdle();
                Thread.sleep(25);
            }

            i++;
        }

        ctx.collect(new HashMap<>());
        ctx.markAsTemporarilyIdle();

        while (!cancelled) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    public GridFSCsvSource lineDelimiter(final char lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
        return this;
    }

    public GridFSCsvSource fieldDelimiter(final char fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
        return this;
    }
}
