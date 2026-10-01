package com.oracle.mongo2ora.migration.converter;

import oracle.jdbc.ErrorSet;
import oracle.jdbc.LargeObjectAccessMode;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.internal.OracleBlob;
import oracle.sql.BlobDBAccess;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public class MyBLOB implements OracleBlob {
	private byte[] data;

	public MyBLOB(final byte[] data) {
		this.data = data;
	}

	@Override
	public long length() throws SQLException {
		return data.length;
	}

	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		return new byte[0];
	}

	@Override
	public InputStream getBinaryStream() throws SQLException {
		return new ByteArrayInputStream(data);
	}

	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		return 0;
	}

	@Override
	public long position(Blob pattern, long start) throws SQLException {
		return 0;
	}

	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		return 0;
	}

	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
		return 0;
	}

	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		return null;
	}

	@Override
	public void truncate(long len) throws SQLException {
	}

	@Override
	public void free() throws SQLException {
	}

	@Override
	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		return null;
	}

	@Override
	public Object toJdbc() throws SQLException {
		return null;
	}

	@Override
	public boolean isConvertibleTo(Class<?> aClass) {
		return false;
	}

	@Override
	public int putBytes(long l, byte[] bytes) throws SQLException {
		return 0;
	}

	@Override
	public int putBytes(long l, byte[] bytes, int i) throws SQLException {
		return 0;
	}

	@Override
	public OutputStream getBinaryOutputStream() throws SQLException {
		return null;
	}

	@Override
	public OutputStream getBinaryOutputStream(long l) throws SQLException {
		return null;
	}

	@Override
	public byte[] shareBytes() {
		return new byte[0];
	}

	@Override
	public long getLength() {
		return 0;
	}

	@Override
	public void setBytes(byte[] bytes) {
		this.data = bytes;
	}

	@Override
	public void setShareBytes(byte[] bytes) {

	}

	@Override
	public byte[] getBytes() {
		return data;
	}

	@Override
	public InputStream getStream() throws SQLException {
		return null;
	}

	@Override
	public String stringValue() throws SQLException {
		return null;
	}

	@Override
	public String stringValue(Connection connection) throws SQLException {
		return null;
	}

	@Override
	public boolean booleanValue() throws SQLException {
		return false;
	}

	@Override
	public int intValue() throws SQLException {
		return 0;
	}

	@Override
	public long longValue() throws SQLException {
		return 0;
	}

	@Override
	public float floatValue() throws SQLException {
		return 0;
	}

	@Override
	public double doubleValue() throws SQLException {
		return 0;
	}

	@Override
	public byte byteValue() throws SQLException {
		return 0;
	}

	@Override
	public BigDecimal bigDecimalValue() throws SQLException {
		return null;
	}

	@Override
	public Date dateValue() throws SQLException {
		return null;
	}

	@Override
	public Time timeValue() throws SQLException {
		return null;
	}

	@Override
	public Time timeValue(Calendar calendar) throws SQLException {
		return null;
	}

	@Override
	public Timestamp timestampValue() throws SQLException {
		return null;
	}

	@Override
	public Timestamp timestampValue(Calendar calendar) throws SQLException {
		return null;
	}

	@Override
	public Reader characterStreamValue() throws SQLException {
		return null;
	}

	@Override
	public InputStream asciiStreamValue() throws SQLException {
		return null;
	}

	@Override
	public InputStream binaryStreamValue() throws SQLException {
		return null;
	}

	@Override
	public InputStream binaryStreamValue(boolean b) throws SQLException {
		return null;
	}

	@Override
	public byte[] getLocator() {
		return new byte[0];
	}

	@Override
	public void setLocator(byte[] bytes) {

	}

	@Override
	public int getChunkSize() throws SQLException {
		return 0;
	}

	@Override
	public int getBufferSize() throws SQLException {
		return 0;
	}

	@Override
	public void trim(long l) throws SQLException {

	}

	@Override
	public Object makeJdbcArray(int i) {
		return null;
	}

	@Override
	public BlobDBAccess getDBAccess() throws SQLException {
		return null;
	}

	@Override
	public Connection getJavaSqlConnection() throws SQLException {
		return null;
	}

	@Override
	public OracleConnection getOracleConnection() throws SQLException {
		return null;
	}

	@Override
	public oracle.jdbc.internal.OracleConnection getInternalConnection() throws SQLException {
		return null;
	}

	@Override
	public oracle.jdbc.driver.OracleConnection getConnection() throws SQLException {
		return null;
	}

	@Override
	public void setPhysicalConnectionOf(Connection connection) {

	}

	@Override
	public void setLength(long l) {

	}

	@Override
	public void setChunkSize(int i) {

	}

	@Override
	public void setActivePrefetch(boolean b) {

	}

	@Override
	public void clearCachedData() {

	}

	@Override
	public boolean isActivePrefetch() {
		return false;
	}

	@Override
	public boolean canReadBasicLobDataInLocator() throws SQLException {
		return false;
	}

	@Override
	public long lengthInternal() throws SQLException {
		return 0;
	}

	@Override
	public CompletionStage<Long> lengthInternalAsync() {
		return null;
	}

	@Override
	public Flow.Publisher<byte[]> publisherOracle(long position) throws SQLException {
		return OracleBlob.super.publisherOracle(position);
	}

	@Override
	public Flow.Subscriber<byte[]> subscriberOracle(long position) throws SQLException {
		return OracleBlob.super.subscriberOracle(position);
	}

	@Override
	public Flow.Subscriber<byte[]> subscriberOracle(long position, Flow.Subscriber<Long> outcomeSubscriber) throws SQLException {
		return OracleBlob.super.subscriberOracle(position, outcomeSubscriber);
	}

	@Override
	public void openLob(LargeObjectAccessMode mode) throws SQLException {
	}

	@Override
	public void closeLob() throws SQLException {
	}

	@Override
	public boolean isOpenLob() throws SQLException {
		return false;
	}

	@Override
	public int getBytes(long l, int i, byte[] bytes) throws SQLException {
		return 0;
	}

	@Override
	public boolean isEmptyLob() throws SQLException {
		return false;
	}

	@Override
	public boolean isSecureFile() throws SQLException {
		return false;
	}

	@Override
	public InputStream getBinaryStream(long l) throws SQLException {
		return null;
	}

	@Override
	public void freeTemporary() throws SQLException {

	}

	@Override
	public void freeLOB() throws SQLException {

	}

	@Override
	public boolean isTemporary() throws SQLException {
		return false;
	}

	@Override
	public short getDuration() throws SQLException {
		return 0;
	}

	@Override
	public boolean isFree() {
		return false;
	}

	@Override
	public void setPrefetchData(PrefetchData<byte[]> prefetchData) {

	}

	@Override
	public PrefetchData<byte[]> getPrefetchData() {
		return null;
	}

	@Override
	public SQLXML toSQLXML() throws SQLException {
		return null;
	}

	@Override
	public SQLXML toSQLXML(int i) throws SQLException {
		return null;
	}

	@Override
	public Flow.Publisher<byte[]> publisherOracle(long position, ErrorSet continueOnErrorSet) throws SQLException {
		return null;
	}

	@Override
	public Flow.Subscriber<byte[]> subscriberOracle(long position, ErrorSet continueOnErrorSet) throws SQLException {
		return null;
	}

	@Override
	public Flow.Subscriber<byte[]> subscriberOracle(long position, Flow.Subscriber<Long> outcomeSubscriber, ErrorSet continueOnErrorSet) throws SQLException {
		return null;
	}

	@Override
	public Flow.Publisher<Void> freeAsyncOracle() throws SQLException {
		return null;
	}

	@Override
	public void setACProxy(Object o) {

	}

	@Override
	public Object getACProxy() {
		return null;
	}
}

