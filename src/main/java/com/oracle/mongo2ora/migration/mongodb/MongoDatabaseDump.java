package com.oracle.mongo2ora.migration.mongodb;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.CreateViewOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;


public class MongoDatabaseDump implements MongoDatabase {
	private final String sourceDumpFolderName;
	private final File sourceDumpFolder;

	public MongoDatabaseDump(String sourceDumpFolder) {
		this.sourceDumpFolderName = sourceDumpFolder;
		this.sourceDumpFolder = new File(sourceDumpFolder);
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public CodecRegistry getCodecRegistry() {
		return null;
	}

	@Override
	public ReadPreference getReadPreference() {
		return null;
	}

	@Override
	public WriteConcern getWriteConcern() {
		return null;
	}

	@Override
	public ReadConcern getReadConcern() {
		return null;
	}

	@Override
	public MongoDatabase withCodecRegistry(CodecRegistry codecRegistry) {
		return null;
	}

	@Override
	public MongoDatabase withReadPreference(ReadPreference readPreference) {
		return null;
	}

	@Override
	public MongoDatabase withWriteConcern(WriteConcern writeConcern) {
		return null;
	}

	@Override
	public MongoDatabase withReadConcern(ReadConcern readConcern) {
		return null;
	}

	@Override
	public MongoCollection<Document> getCollection(String s) {
		return null;
	}

	@Override
	public <TDocument> MongoCollection<TDocument> getCollection(String name, Class<TDocument> documentClass) {
		return new MongoCollectionDump(sourceDumpFolder, name, documentClass);
	}

	@Override
	public Document runCommand(Bson bson) {
		return null;
	}

	@Override
	public Document runCommand(Bson bson, ReadPreference readPreference) {
		return null;
	}

	@Override
	public <TResult> TResult runCommand(Bson bson, Class<TResult> aClass) {
		return null;
	}

	@Override
	public <TResult> TResult runCommand(Bson bson, ReadPreference readPreference, Class<TResult> aClass) {
		return null;
	}

	@Override
	public Document runCommand(ClientSession clientSession, Bson bson) {
		return null;
	}

	@Override
	public Document runCommand(ClientSession clientSession, Bson bson, ReadPreference readPreference) {
		return null;
	}

	@Override
	public <TResult> TResult runCommand(ClientSession clientSession, Bson bson, Class<TResult> aClass) {
		return null;
	}

	@Override
	public <TResult> TResult runCommand(ClientSession clientSession, Bson bson, ReadPreference readPreference, Class<TResult> aClass) {
		return null;
	}

	@Override
	public void drop() {

	}

	@Override
	public void drop(ClientSession clientSession) {

	}

	@Override
	public MongoIterable<String> listCollectionNames() {
		return null;
	}

	@Override
	public ListCollectionsIterable<Document> listCollections() {
		return null;
	}

	public String[] listCollectionsDump() {
		final List<String> result = new ArrayList<>();

		for(String f: Objects.requireNonNull(sourceDumpFolder.list())) {
			if(f.toLowerCase().endsWith(".bson")) {
				result.add(f.substring(0,f.toLowerCase().lastIndexOf(".bson")));
			}
		}

		return result.toArray(new String[0]);
	}

	@Override
	public <TResult> ListCollectionsIterable<TResult> listCollections(Class<TResult> aClass) {
		return null;
	}

	@Override
	public MongoIterable<String> listCollectionNames(ClientSession clientSession) {
		return null;
	}

	@Override
	public ListCollectionsIterable<Document> listCollections(ClientSession clientSession) {
		return null;
	}

	@Override
	public <TResult> ListCollectionsIterable<TResult> listCollections(ClientSession clientSession, Class<TResult> aClass) {
		return null;
	}

	@Override
	public void createCollection(String s) {

	}

	@Override
	public void createCollection(String s, CreateCollectionOptions createCollectionOptions) {

	}

	@Override
	public void createCollection(ClientSession clientSession, String s) {

	}

	@Override
	public void createCollection(ClientSession clientSession, String s, CreateCollectionOptions createCollectionOptions) {

	}

	@Override
	public void createView(String s, String s1, List<? extends Bson> list) {

	}

	@Override
	public void createView(String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {

	}

	@Override
	public void createView(ClientSession clientSession, String s, String s1, List<? extends Bson> list) {

	}

	@Override
	public void createView(ClientSession clientSession, String s, String s1, List<? extends Bson> list, CreateViewOptions createViewOptions) {

	}

	@Override
	public ChangeStreamIterable<Document> watch() {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<Document> watch(List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<Document> watch(ClientSession clientSession) {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public AggregateIterable<Document> aggregate(List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public AggregateIterable<Document> aggregate(ClientSession clientSession, List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession, List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	public int getNumberOfIndexesForCollection(String c) {
		File collectionMetadata= new File(sourceDumpFolder,c+".metadata.json");
		if(!collectionMetadata.exists()) {
			collectionMetadata = new File(sourceDumpFolder, c + ".metadata.json.gz");
			if (!collectionMetadata.exists()) return 0;
		}
		final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		final SimpleModule indexKeyModule = new SimpleModule();
		indexKeyModule.addDeserializer(MetadataKey.class, new MetadataKeyDeserializer());
		mapper.registerModule(indexKeyModule);
		try (InputStream inputStream = collectionMetadata.getName().toLowerCase().endsWith(".gz") ?
				new GZIPInputStream(new FileInputStream(collectionMetadata), 16 * 1024)
				: new BufferedInputStream(new FileInputStream(collectionMetadata), 16 * 1024)) {
			MongoDBMetadata mongoDBMetadata = mapper.readValue(inputStream, MongoDBMetadata.class);

			return mongoDBMetadata.getIndexes().length;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}
	public MongoDBMetadata getCollectionMetadata(String c) {
		File collectionMetadata= new File(sourceDumpFolder,c+".metadata.json");
		if(!collectionMetadata.exists()) {
			collectionMetadata= new File(sourceDumpFolder,c+".metadata.json.gz");
			if(!collectionMetadata.exists()) return null;
		}

		final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		final SimpleModule indexKeyModule = new SimpleModule();
		indexKeyModule.addDeserializer(MetadataKey.class, new MetadataKeyDeserializer());
		mapper.registerModule(indexKeyModule);
		try (InputStream inputStream = collectionMetadata.getName().toLowerCase().endsWith(".gz") ?
				new GZIPInputStream(new FileInputStream(collectionMetadata), 16 * 1024)
				: new BufferedInputStream(new FileInputStream(collectionMetadata), 16 * 1024)) {
			return mapper.readValue(inputStream, MongoDBMetadata.class);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public File getBSONFile(String c) {
		File collectionData= new File(sourceDumpFolder,c+".bson");
		if(!collectionData.exists()) {
			collectionData= new File(sourceDumpFolder,c+".bson.gz");
			if(!collectionData.exists()) return null;
		}

		return collectionData;
	}
}
