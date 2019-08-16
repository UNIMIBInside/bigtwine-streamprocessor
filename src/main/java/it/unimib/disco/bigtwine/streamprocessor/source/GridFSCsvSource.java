package it.unimib.disco.bigtwine.streamprocessor.source;

import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.types.ObjectId;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class GridFSCsvSource implements SourceFunction<Tuple3<Double, Boolean, Map<String, String>>> {
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
    public void run(SourceContext<Tuple3<Double, Boolean, Map<String, String>>> ctx) throws Exception {
        GridFSDBFile file = this.getFs().findOne(this.objectId);
        long fileLength = file.getLength();
        Reader reader = new InputStreamReader(file.getInputStream());

        CSVParser parser = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter(fieldDelimiter)
                .withRecordSeparator(lineDelimiter)
                .parse(reader);
        Iterator<CSVRecord> records = parser.iterator();

        Random random = new Random();
        while (records.hasNext()) {
            if (cancelled) {
                break;
            }
            CSVRecord record = records.next();
            boolean isLast = !records.hasNext();
            double progress = isLast ? 1 : (record.getCharacterPosition() / (double)fileLength);

            Tuple3<Double, Boolean, Map<String, String>> out = new Tuple3<Double, Boolean, Map<String, String>>();
            out.f0 = progress;
            out.f1 = isLast;
            out.f2 = record.toMap();

            ctx.collect(out);
            Thread.sleep(random.nextInt(200));
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
