package org.bson;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MyBSONDecoder {
	protected final boolean outputOsonFormat;
	protected int bsonLength;
	protected String oid;
	private final MyBSON2OSONWriter writer = new MyBSON2OSONWriter();
	//private final MyBSON2NullWriter writer = new MyBSON2NullWriter();
	private final MyBSONReader reader = new MyBSONReader();

	public MyBSONDecoder(boolean outputOsonFormat) {
		this.outputOsonFormat = outputOsonFormat;
	}

	public void convertBSONToOSON(final RawBsonDocument doc) {
		//System.out.println(doc);
		reader.reset(doc);
		writer.reset();
		writer.pipe(reader);
		bsonLength = reader.getBsonInput().getPosition();
	}

	public final byte[] getOSONData() {
		return writer.getOSONBytes();
	}

	public int getBsonLength() {
		return bsonLength;
	}

	public final String getOid() {
		return writer.getOid();
	}


}
