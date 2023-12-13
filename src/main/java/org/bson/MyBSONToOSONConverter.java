package org.bson;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.oracle.mongo2ora.migration.mongodb.CollectionClusteringAnalyzer.useIdIndexHint;

public class MyBSONToOSONConverter {
	private final boolean relativeOffsets;
	private final boolean lastValueSharing;
	private final boolean simpleValueSharing;
	private boolean allowDuplicateKeys;
	protected long bsonLength;
	protected String oid;
	private final MyBSON2OSONWriter writer = new MyBSON2OSONWriter();
	//private final MyBSON2NullWriter writer = new MyBSON2NullWriter();
	private final MyBSONReader reader = new MyBSONReader();

	public MyBSONToOSONConverter() {
		this.relativeOffsets = this.lastValueSharing = this.simpleValueSharing = false;
	}

	public MyBSONToOSONConverter(boolean allowDuplicateKeys, boolean relativeOffsets, boolean lastValueSharing, boolean simpleValueSharing) {
		this.allowDuplicateKeys = allowDuplicateKeys;
		this.relativeOffsets = relativeOffsets;
		this.lastValueSharing = lastValueSharing;
		this.simpleValueSharing = simpleValueSharing;
	}

	public void convertBSONToOSON(final RawBsonDocument doc) {
		//doc.getByteBuffer()
		//System.out.println(doc);
/*		reader.reset(doc);
		writer.reset(allowDuplicateKeys, relativeOffsets, lastValueSharing, simpleValueSharing);
		writer.pipe(reader);
		bsonLength = reader.getBsonInput().getPosition();*/
	}

	public long getKeysSize() {
		//return writer.getKeysSize();
		return 0;
	}

	public final byte[] getOSONData() {
		return new byte[0];//writer.getOSONBytes();
	}

	public long getBsonLength() {
		return bsonLength;
	}

	public final String getOid() {
		return ""; //writer.getOid();
	}

	public final boolean hasOid() {
		return true; //writer.hasOid();
	}

	public static void main(String[] args) throws Throwable {
		final MongoClientSettings settings =
				MongoClientSettings.builder()
						.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
						.credential(MongoCredential.createCredential("moviestream", "moviestream", "My_Strong_Pa55word".toCharArray()))
						.applyToConnectionPoolSettings(builder ->
								builder.maxSize(10).minSize(10).maxConnecting(10).maxWaitTime(1,TimeUnit.DAYS).maxConnectionIdleTime(10, TimeUnit.MINUTES))
						.applyToClusterSettings(builder ->
								builder.hosts(Arrays.asList(new ServerAddress("localhost", 27016))))
						.build();

		try (MongoClient mongoClient = MongoClients.create(settings)) {
			MongoDatabase mongoDatabase = mongoClient.getDatabase("moviestream");
			MongoCollection<MyBsonDocument> collection = mongoDatabase.getCollection("customers", MyBsonDocument.class);

			try (MongoCursor<MyBsonDocument> cursor = collection.find().hint(useIdIndexHint).batchSize(100).cursor()) {
				while (cursor.hasNext()) {
					final MyBsonDocument doc = cursor.next();

					System.out.println(doc.toJson());
				}
			}
		}

		String j = Files.readString(new File("test2.json").toPath());

		MyBSONToOSONConverter dec = new MyBSONToOSONConverter( true, true, true, true);
		dec.convertBSONToOSON(RawBsonDocument.parse(j));

		FileOutputStream o = new FileOutputStream("test.oson.out");
		o.write(dec.getOSONData());
		o.close();

	}
}
