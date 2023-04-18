package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MapReduceIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.DropIndexOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.File;
import java.util.List;

public class MongoCollectionDump<TDocument> implements MongoCollection<TDocument> {
	public final File sourceDumpFolder;
	public final String name;
	public final Class<TDocument> documentClass;
	public CollectionCluster work;

	public <TDocument> MongoCollectionDump(File sourceDumpFolder, String name, Class<TDocument> documentClass) {
		this.sourceDumpFolder = sourceDumpFolder;
		this.name=name;
		this.documentClass=(Class)documentClass;
	}

	@Override
	public MongoNamespace getNamespace() {
		return null;
	}

	@Override
	public Class<TDocument> getDocumentClass() {
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
	public <NewTDocument> MongoCollection<NewTDocument> withDocumentClass(Class<NewTDocument> aClass) {
		return null;
	}

	@Override
	public MongoCollection<TDocument> withCodecRegistry(CodecRegistry codecRegistry) {
		return null;
	}

	@Override
	public MongoCollection<TDocument> withReadPreference(ReadPreference readPreference) {
		return null;
	}

	@Override
	public MongoCollection<TDocument> withWriteConcern(WriteConcern writeConcern) {
		return null;
	}

	@Override
	public MongoCollection<TDocument> withReadConcern(ReadConcern readConcern) {
		return null;
	}

	@Override
	public long countDocuments() {
		return 0;
	}

	@Override
	public long countDocuments(Bson bson) {
		return 0;
	}

	@Override
	public long countDocuments(Bson bson, CountOptions countOptions) {
		return 0;
	}

	@Override
	public long countDocuments(ClientSession clientSession) {
		return 0;
	}

	@Override
	public long countDocuments(ClientSession clientSession, Bson bson) {
		return 0;
	}

	@Override
	public long countDocuments(ClientSession clientSession, Bson bson, CountOptions countOptions) {
		return 0;
	}

	@Override
	public long estimatedDocumentCount() {
		return 0;
	}

	@Override
	public long estimatedDocumentCount(EstimatedDocumentCountOptions estimatedDocumentCountOptions) {
		return 0;
	}

	@Override
	public <TResult> DistinctIterable<TResult> distinct(String s, Class<TResult> aClass) {
		return null;
	}

	@Override
	public <TResult> DistinctIterable<TResult> distinct(String s, Bson bson, Class<TResult> aClass) {
		return null;
	}

	@Override
	public <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String s, Class<TResult> aClass) {
		return null;
	}

	@Override
	public <TResult> DistinctIterable<TResult> distinct(ClientSession clientSession, String s, Bson bson, Class<TResult> aClass) {
		return null;
	}

	@Override
	public FindIterable<TDocument> find() {
		return null;
	}

	@Override
	public <TResult> FindIterable<TResult> find(Class<TResult> aClass) {
		return null;
	}

	@Override
	public FindIterable<TDocument> find(Bson bson) {
		return new FindIterableDump<>(this);
	}

	@Override
	public <TResult> FindIterable<TResult> find(Bson bson, Class<TResult> aClass) {
		return null;
	}

	@Override
	public FindIterable<TDocument> find(ClientSession clientSession) {
		return null;
	}

	@Override
	public <TResult> FindIterable<TResult> find(ClientSession clientSession, Class<TResult> aClass) {
		return null;
	}

	@Override
	public FindIterable<TDocument> find(ClientSession clientSession, Bson bson) {
		return null;
	}

	@Override
	public <TResult> FindIterable<TResult> find(ClientSession clientSession, Bson bson, Class<TResult> aClass) {
		return null;
	}

	@Override
	public AggregateIterable<TDocument> aggregate(List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> AggregateIterable<TResult> aggregate(List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public AggregateIterable<TDocument> aggregate(ClientSession clientSession, List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> AggregateIterable<TResult> aggregate(ClientSession clientSession, List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<TDocument> watch() {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<TDocument> watch(List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<TDocument> watch(ClientSession clientSession) {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> aClass) {
		return null;
	}

	@Override
	public ChangeStreamIterable<TDocument> watch(ClientSession clientSession, List<? extends Bson> list) {
		return null;
	}

	@Override
	public <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> list, Class<TResult> aClass) {
		return null;
	}

	@Override
	public MapReduceIterable<TDocument> mapReduce(String s, String s1) {
		return null;
	}

	@Override
	public <TResult> MapReduceIterable<TResult> mapReduce(String s, String s1, Class<TResult> aClass) {
		return null;
	}

	@Override
	public MapReduceIterable<TDocument> mapReduce(ClientSession clientSession, String s, String s1) {
		return null;
	}

	@Override
	public <TResult> MapReduceIterable<TResult> mapReduce(ClientSession clientSession, String s, String s1, Class<TResult> aClass) {
		return null;
	}

	@Override
	public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends TDocument>> list) {
		return null;
	}

	@Override
	public BulkWriteResult bulkWrite(List<? extends WriteModel<? extends TDocument>> list, BulkWriteOptions bulkWriteOptions) {
		return null;
	}

	@Override
	public BulkWriteResult bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> list) {
		return null;
	}

	@Override
	public BulkWriteResult bulkWrite(ClientSession clientSession, List<? extends WriteModel<? extends TDocument>> list, BulkWriteOptions bulkWriteOptions) {
		return null;
	}

	@Override
	public InsertOneResult insertOne(TDocument tDocument) {
		return null;
	}

	@Override
	public InsertOneResult insertOne(TDocument tDocument, InsertOneOptions insertOneOptions) {
		return null;
	}

	@Override
	public InsertOneResult insertOne(ClientSession clientSession, TDocument tDocument) {
		return null;
	}

	@Override
	public InsertOneResult insertOne(ClientSession clientSession, TDocument tDocument, InsertOneOptions insertOneOptions) {
		return null;
	}

	@Override
	public InsertManyResult insertMany(List<? extends TDocument> list) {
		return null;
	}

	@Override
	public InsertManyResult insertMany(List<? extends TDocument> list, InsertManyOptions insertManyOptions) {
		return null;
	}

	@Override
	public InsertManyResult insertMany(ClientSession clientSession, List<? extends TDocument> list) {
		return null;
	}

	@Override
	public InsertManyResult insertMany(ClientSession clientSession, List<? extends TDocument> list, InsertManyOptions insertManyOptions) {
		return null;
	}

	@Override
	public DeleteResult deleteOne(Bson bson) {
		return null;
	}

	@Override
	public DeleteResult deleteOne(Bson bson, DeleteOptions deleteOptions) {
		return null;
	}

	@Override
	public DeleteResult deleteOne(ClientSession clientSession, Bson bson) {
		return null;
	}

	@Override
	public DeleteResult deleteOne(ClientSession clientSession, Bson bson, DeleteOptions deleteOptions) {
		return null;
	}

	@Override
	public DeleteResult deleteMany(Bson bson) {
		return null;
	}

	@Override
	public DeleteResult deleteMany(Bson bson, DeleteOptions deleteOptions) {
		return null;
	}

	@Override
	public DeleteResult deleteMany(ClientSession clientSession, Bson bson) {
		return null;
	}

	@Override
	public DeleteResult deleteMany(ClientSession clientSession, Bson bson, DeleteOptions deleteOptions) {
		return null;
	}

	@Override
	public UpdateResult replaceOne(Bson bson, TDocument tDocument) {
		return null;
	}

	@Override
	public UpdateResult replaceOne(Bson bson, TDocument tDocument, ReplaceOptions replaceOptions) {
		return null;
	}

	@Override
	public UpdateResult replaceOne(ClientSession clientSession, Bson bson, TDocument tDocument) {
		return null;
	}

	@Override
	public UpdateResult replaceOne(ClientSession clientSession, Bson bson, TDocument tDocument, ReplaceOptions replaceOptions) {
		return null;
	}

	@Override
	public UpdateResult updateOne(Bson bson, Bson bson1) {
		return null;
	}

	@Override
	public UpdateResult updateOne(Bson bson, Bson bson1, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateOne(ClientSession clientSession, Bson bson, Bson bson1) {
		return null;
	}

	@Override
	public UpdateResult updateOne(ClientSession clientSession, Bson bson, Bson bson1, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateOne(Bson bson, List<? extends Bson> list) {
		return null;
	}

	@Override
	public UpdateResult updateOne(Bson bson, List<? extends Bson> list, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateOne(ClientSession clientSession, Bson bson, List<? extends Bson> list) {
		return null;
	}

	@Override
	public UpdateResult updateOne(ClientSession clientSession, Bson bson, List<? extends Bson> list, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateMany(Bson bson, Bson bson1) {
		return null;
	}

	@Override
	public UpdateResult updateMany(Bson bson, Bson bson1, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateMany(ClientSession clientSession, Bson bson, Bson bson1) {
		return null;
	}

	@Override
	public UpdateResult updateMany(ClientSession clientSession, Bson bson, Bson bson1, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateMany(Bson bson, List<? extends Bson> list) {
		return null;
	}

	@Override
	public UpdateResult updateMany(Bson bson, List<? extends Bson> list, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public UpdateResult updateMany(ClientSession clientSession, Bson bson, List<? extends Bson> list) {
		return null;
	}

	@Override
	public UpdateResult updateMany(ClientSession clientSession, Bson bson, List<? extends Bson> list, UpdateOptions updateOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndDelete(Bson bson) {
		return null;
	}

	@Override
	public TDocument findOneAndDelete(Bson bson, FindOneAndDeleteOptions findOneAndDeleteOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndDelete(ClientSession clientSession, Bson bson) {
		return null;
	}

	@Override
	public TDocument findOneAndDelete(ClientSession clientSession, Bson bson, FindOneAndDeleteOptions findOneAndDeleteOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndReplace(Bson bson, TDocument tDocument) {
		return null;
	}

	@Override
	public TDocument findOneAndReplace(Bson bson, TDocument tDocument, FindOneAndReplaceOptions findOneAndReplaceOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndReplace(ClientSession clientSession, Bson bson, TDocument tDocument) {
		return null;
	}

	@Override
	public TDocument findOneAndReplace(ClientSession clientSession, Bson bson, TDocument tDocument, FindOneAndReplaceOptions findOneAndReplaceOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(Bson bson, Bson bson1) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(Bson bson, Bson bson1, FindOneAndUpdateOptions findOneAndUpdateOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(ClientSession clientSession, Bson bson, Bson bson1) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(ClientSession clientSession, Bson bson, Bson bson1, FindOneAndUpdateOptions findOneAndUpdateOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(Bson bson, List<? extends Bson> list) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(Bson bson, List<? extends Bson> list, FindOneAndUpdateOptions findOneAndUpdateOptions) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(ClientSession clientSession, Bson bson, List<? extends Bson> list) {
		return null;
	}

	@Override
	public TDocument findOneAndUpdate(ClientSession clientSession, Bson bson, List<? extends Bson> list, FindOneAndUpdateOptions findOneAndUpdateOptions) {
		return null;
	}

	@Override
	public void drop() {

	}

	@Override
	public void drop(ClientSession clientSession) {

	}

	@Override
	public String createIndex(Bson bson) {
		return null;
	}

	@Override
	public String createIndex(Bson bson, IndexOptions indexOptions) {
		return null;
	}

	@Override
	public String createIndex(ClientSession clientSession, Bson bson) {
		return null;
	}

	@Override
	public String createIndex(ClientSession clientSession, Bson bson, IndexOptions indexOptions) {
		return null;
	}

	@Override
	public List<String> createIndexes(List<IndexModel> list) {
		return null;
	}

	@Override
	public List<String> createIndexes(List<IndexModel> list, CreateIndexOptions createIndexOptions) {
		return null;
	}

	@Override
	public List<String> createIndexes(ClientSession clientSession, List<IndexModel> list) {
		return null;
	}

	@Override
	public List<String> createIndexes(ClientSession clientSession, List<IndexModel> list, CreateIndexOptions createIndexOptions) {
		return null;
	}

	@Override
	public ListIndexesIterable<Document> listIndexes() {
		return null;
	}

	@Override
	public <TResult> ListIndexesIterable<TResult> listIndexes(Class<TResult> aClass) {
		return null;
	}

	@Override
	public ListIndexesIterable<Document> listIndexes(ClientSession clientSession) {
		return null;
	}

	@Override
	public <TResult> ListIndexesIterable<TResult> listIndexes(ClientSession clientSession, Class<TResult> aClass) {
		return null;
	}

	@Override
	public void dropIndex(String s) {

	}

	@Override
	public void dropIndex(String s, DropIndexOptions dropIndexOptions) {

	}

	@Override
	public void dropIndex(Bson bson) {

	}

	@Override
	public void dropIndex(Bson bson, DropIndexOptions dropIndexOptions) {

	}

	@Override
	public void dropIndex(ClientSession clientSession, String s) {

	}

	@Override
	public void dropIndex(ClientSession clientSession, Bson bson) {

	}

	@Override
	public void dropIndex(ClientSession clientSession, String s, DropIndexOptions dropIndexOptions) {

	}

	@Override
	public void dropIndex(ClientSession clientSession, Bson bson, DropIndexOptions dropIndexOptions) {

	}

	@Override
	public void dropIndexes() {

	}

	@Override
	public void dropIndexes(ClientSession clientSession) {

	}

	@Override
	public void dropIndexes(DropIndexOptions dropIndexOptions) {

	}

	@Override
	public void dropIndexes(ClientSession clientSession, DropIndexOptions dropIndexOptions) {

	}

	@Override
	public void renameCollection(MongoNamespace mongoNamespace) {

	}

	@Override
	public void renameCollection(MongoNamespace mongoNamespace, RenameCollectionOptions renameCollectionOptions) {

	}

	@Override
	public void renameCollection(ClientSession clientSession, MongoNamespace mongoNamespace) {

	}

	@Override
	public void renameCollection(ClientSession clientSession, MongoNamespace mongoNamespace, RenameCollectionOptions renameCollectionOptions) {

	}

	public void setWork(CollectionCluster work) {
		this.work = work;
	}
}
