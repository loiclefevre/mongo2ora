package com.oracle.mongo2ora.migration.mongodb;


import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;


public class MongoDatabaseDump {
	private final String sourceDumpFolderName;
	private final File sourceDumpFolder;

	public MongoDatabaseDump(String sourceDumpFolder) {
		this.sourceDumpFolderName = sourceDumpFolder;
		this.sourceDumpFolder = new File(sourceDumpFolder);
	}

	public String[] listCollections() {
		final List<String> result = new ArrayList<>();

		for(String f: Objects.requireNonNull(sourceDumpFolder.list())) {
			if(f.toLowerCase().endsWith(".bson")) {
				result.add(f.substring(0,f.toLowerCase().lastIndexOf(".bson")));
			}
		}

		return result.toArray(new String[0]);
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
}
