package com.oracle.mongo2ora.migration.converter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class MyBLOB implements Blob {

	private final byte[] data;

	public MyBLOB(final byte[] data) {
		this.data = data;
	}

	@Override
	public long length() throws SQLException {
		System.out.println("length()");
		return data.length;
	}

	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		System.out.println("public byte[] getBytes(long pos, int length) throws SQLException");
		return new byte[0];
	}

	@Override
	public InputStream getBinaryStream() throws SQLException {
		System.out.println("public InputStream getBinaryStream() throws SQLException");
		return new ByteArrayInputStream(data);
	}

	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		System.out.println("public long position(byte[] pattern, long start) throws SQLException");
		return 0;
	}

	@Override
	public long position(Blob pattern, long start) throws SQLException {
		System.out.println("public long position(Blob pattern, long start) throws SQLException");
		return 0;
	}

	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		System.out.println("public int setBytes(long pos, byte[] bytes) throws SQLException");
		return 0;
	}

	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		System.out.println("public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException");
		return 0;
	}

	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		System.out.println("public OutputStream setBinaryStream(long pos) throws SQLException");
		return null;
	}

	@Override
	public void truncate(long len) throws SQLException {
		System.out.println("public void truncate(long len) throws SQLException");
	}

	@Override
	public void free() throws SQLException {
		System.out.println("public void free() throws SQLException");
	}

	@Override
	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		System.out.println("public InputStream getBinaryStream(long pos, long length) throws SQLException");
		return null;
	}
}
