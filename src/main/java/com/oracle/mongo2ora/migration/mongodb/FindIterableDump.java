package com.oracle.mongo2ora.migration.mongodb;

import com.mongodb.CursorType;
import com.mongodb.ExplainVerbosity;
import com.mongodb.Function;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class FindIterableDump<TDocument> implements FindIterable<TDocument>  {
	public final MongoCollectionDump<TDocument> mongoCollectionDump;

	public FindIterableDump(MongoCollectionDump<TDocument> mongoCollectionDump) {
		this.mongoCollectionDump = mongoCollectionDump;
	}

	@Override
	public FindIterable<TDocument> filter(Bson bson) {
		return null;
	}

	@Override
	public FindIterable<TDocument> limit(int i) {
		return null;
	}

	@Override
	public FindIterable<TDocument> skip(int i) {
		return null;
	}

	@Override
	public FindIterable<TDocument> maxTime(long l, TimeUnit timeUnit) {
		return null;
	}

	@Override
	public FindIterable<TDocument> maxAwaitTime(long l, TimeUnit timeUnit) {
		return null;
	}

	@Override
	public FindIterable<TDocument> projection(Bson bson) {
		return null;
	}

	@Override
	public FindIterable<TDocument> sort(Bson bson) {
		return null;
	}

	@Override
	public FindIterable<TDocument> noCursorTimeout(boolean b) {
		return null;
	}

	@Override
	public FindIterable<TDocument> oplogReplay(boolean b) {
		return null;
	}

	@Override
	public FindIterable<TDocument> partial(boolean b) {
		return null;
	}

	@Override
	public FindIterable<TDocument> cursorType(CursorType cursorType) {
		return null;
	}

	@Override
	public MongoCursor<TDocument> iterator() {
		return new MongoCursorDump<>(this);
	}

	@Override
	public MongoCursor<TDocument> cursor() {
		return this.iterator();
	}

	@Override
	public TDocument first() {
		return null;
	}

	@Override
	public <U> MongoIterable<U> map(Function<TDocument, U> function) {
		return null;
	}

	@Override
	public <A extends Collection<? super TDocument>> A into(A objects) {
		return null;
	}

	@Override
	public FindIterable<TDocument> batchSize(int i) {
		return this;
	}

	@Override
	public FindIterable<TDocument> collation(Collation collation) {
		return null;
	}

	@Override
	public FindIterable<TDocument> comment(String s) {
		return null;
	}

	@Override
	public FindIterable<TDocument> comment(BsonValue bsonValue) {
		return null;
	}

	@Override
	public FindIterable<TDocument> hint(Bson bson) {
		return this;
	}

	@Override
	public FindIterable<TDocument> hintString(String s) {
		return null;
	}

	@Override
	public FindIterable<TDocument> let(Bson bson) {
		return null;
	}

	@Override
	public FindIterable<TDocument> max(Bson bson) {
		return null;
	}

	@Override
	public FindIterable<TDocument> min(Bson bson) {
		return null;
	}

	@Override
	public FindIterable<TDocument> returnKey(boolean b) {
		return null;
	}

	@Override
	public FindIterable<TDocument> showRecordId(boolean b) {
		return null;
	}

	@Override
	public FindIterable<TDocument> allowDiskUse(Boolean aBoolean) {
		return null;
	}

	@Override
	public Document explain() {
		return null;
	}

	@Override
	public Document explain(ExplainVerbosity explainVerbosity) {
		return null;
	}

	@Override
	public <E> E explain(Class<E> aClass) {
		return null;
	}

	@Override
	public <E> E explain(Class<E> aClass, ExplainVerbosity explainVerbosity) {
		return null;
	}
}
