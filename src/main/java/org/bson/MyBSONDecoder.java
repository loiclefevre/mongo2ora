package org.bson;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;

public class MyBSONDecoder {
	protected final boolean outputOsonFormat;
	private final boolean relativeOffsets;
	private final boolean lastValueSharing;
	private final boolean simpleValueSharing;
	private boolean allowDuplicateKeys;
	protected int bsonLength;
	protected String oid;
	private final MyBSON2OSONWriter writer = new MyBSON2OSONWriter();
	//private final MyBSON2NullWriter writer = new MyBSON2NullWriter();
	private final MyBSONReader reader = new MyBSONReader();

	public MyBSONDecoder(boolean outputOsonFormat) {
		this.outputOsonFormat = outputOsonFormat;
		this.relativeOffsets = this.lastValueSharing = this.simpleValueSharing = false;
	}

	public MyBSONDecoder(boolean outputOsonFormat, boolean allowDuplicateKeys, boolean relativeOffsets, boolean lastValueSharing, boolean simpleValueSharing) {
		this.outputOsonFormat = outputOsonFormat;
		this.allowDuplicateKeys = allowDuplicateKeys;
		this.relativeOffsets = relativeOffsets;
		this.lastValueSharing = lastValueSharing;
		this.simpleValueSharing = simpleValueSharing;
	}

	public void convertBSONToOSON(final RawBsonDocument doc) {
		//System.out.println(doc);
		reader.reset(doc);
		writer.reset(allowDuplicateKeys, relativeOffsets, lastValueSharing, simpleValueSharing);
		writer.pipe(reader);
		bsonLength = reader.getBsonInput().getPosition();
	}

	public long getKeysSize() {
		return writer.getKeysSize();
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

	public final boolean hasOid() {
		return writer.hasOid();
	}

	public static void main(String[] args) throws Throwable {

		/*String j="{\"_id\": {\"$oid\": \"61685df979fb4978a7be0a12\"}, \"test\": {\"_id\":{\"$oid\":\"61685df979fb4978a7be0a13\"}}, \"movie_id\": 1, \"sku\": \"COO3790\", \"list_price\": 3.99, \"wiki_article\": \"'Gator_Bait_II:_Cajun_Justice\", \"title\": \"'Gator Bait II: Cajun Justice\", \"opening_date\": \"1988-01-01\", \"year\": 1988, \"views\": 6, \"cast\": null, \"crew\": [{\"job\": \"director\", \"names\": [\"Beverly Sebastion\"]}, {\"job\": \"screenwriter\", \"names\": [\"Beverly Sebastion\"]}], \"studio\": null, \"runtime\": 95, \"budget\": null, \"gross\": null, \"genre\": [\"Thriller\"], \"main_subject\": null, \"awards\": null, \"nominations\": null, \"image_url\": \"https://upload.wikimedia.org/wikipedia/en/9/91/Gatorbait2.jpg\", \"summary\": \"'  Gator Bait II: Cajun Justice is a 1988 sequel to the 1974 film 'Gator Bait , written, produced and directed by Beverly Sebastian and Ferd Sebastian. Largely ignored upon release, the film received a second life on cable television and home video.\"}";*/

		/*String j = "{\"_id\":{\"$oid\":\"6482ed016c008c89fcb2d017\"},\"fechaValor\":{\"$date\":\"2023-06-06T00:00:00Z\"},\"idContrato\":\"004900013003049005\",\"idMov\":\"2023060621032120118000019004900013003049005000658IP29100490001\",\"centroOrigen\":\"0001\",\"codigoOperacion\":\"069\",\"codigoOperacionBancaria\":\"044\",\"codigoOperacionBasica\":\"050\",\"descripcion\":\"BIZUM DE Mirabel Madrigal CONCEPTO Concepto de prueba\",\"descripcionGenerica\":\"APLICACION DE ORDENES\",\"divisa\":\"EUR\",\"empresaOrigen\":\"0049\",\"estado\":\"NR\",\"fechaAnotacion\":{\"$date\":\"2023-06-09T00:00:00Z\"},\"fechaContable\":{\"$date\":\"2023-06-06T00:00:00Z\"},\"fechaOperacion\":{\"$date\":\"2023-06-06T21:03:21.201Z\"},\"importe\":{\"$numberDecimal\":\"0.01\"},\"numDgo\":658,\"numMovimiento\":19,\"numOrden\":19,\"numeroDocumento\":\"\",\"posicionSaldo\":\"000\",\"referencia1\":\"\",\"referencia2\":\"\",\"saldo\":{\"$numberDecimal\":\"18672.88\"},\"signoMovimiento\":\"Haber\",\"terminalBTO\":\"IP291\",\"timestamps\":{\"tsOBMOVTOS1\":2.0231572103212365E+18},\"tipoOperacion\":\"ABONO-TRANSFERENCIA\",\"tsAbInitio\":\"1686078201886\",\"tsHost\":\"1686078201251\",\"transactionInternalReference\":\"004900017650189164\"}";*/
		//String j = "{\"numDgo\":658}";
		//String j = "{\"numDgo\":{\"$numberDecimal\":\"658\"}}";

		String j = Files.readString(new File("test2.json").toPath());

		MyBSONDecoder dec = new MyBSONDecoder(true, true, true, true, true);
		dec.convertBSONToOSON(RawBsonDocument.parse(j));
		FileOutputStream o = new FileOutputStream("test.oson.out");
		o.write(dec.getOSONData());
		o.close();

		RawBsonDocument rawBSON = RawBsonDocument.parse(j);
		o = new FileOutputStream("test.bson.out");
		o.write(rawBSON.getByteBuffer().array(),0,rawBSON.getByteBuffer().limit());
		o.close();

//		String j = "{\"test\":true, \"_id\": {\"$oid\":\"655471d50a6a7c1864ffa194\"}}";
//		MyBSONDecoder dec = new MyBSONDecoder(true);
//		dec.convertBSONToOSON(RawBsonDocument.parse(j));
//		FileOutputStream o = new FileOutputStream("test.oson.out");
//		o.write(dec.getOSONData());
//		o.close();

/*
		Class.forName("oracle.jdbc.driver.OracleDriver");

		try (Connection c = DriverManager.getConnection("jdbc:oracle:thin:@localhost/freepdb1","developer","free")) {
			try (Statement s = c.createStatement()) {
				try (ResultSet r = s.executeQuery("select m.data from movimientos1 m where m.data.\"_id\"='6482ed016c008c89fcb2d017'")) {
					if(r.next()) {
						Blob b = r.getBlob(1);
						Files.copy(b.getBinaryStream(),new File("movimientos1.oson").toPath(), StandardCopyOption.REPLACE_EXISTING);
					}
				}
				try (ResultSet r = s.executeQuery("select to_blob(oson(m.data)) from movimientos_restore m where m.data.\"_id\".string()='6482ed016c008c89fcb2d017'")) {
					if(r.next()) {
						Blob b = r.getBlob(1);
						Files.copy(b.getBinaryStream(),new File("movimientos_restore.oson").toPath(), StandardCopyOption.REPLACE_EXISTING);
					}
				}
			}
		}*/
	}

/*
    public static void main(String[] args) throws Throwable {
        //InputStream in = new FileInputStream(new File("test.bson"));
        //InputStream in = new FileInputStream(new File("autoedit_countdown_tag.bson"));
        InputStream in = new FileInputStream(new File("objectlabs-system.admin.collections.bson"));

        final MyBSONDecoder decoder = new MyBSONDecoder(true);

        final BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(doc.getByteBuffer()));

        while(true) {
            byte[] data = decoder.convertBSONToOSON(new BsonBinaryReader(new BsonF));
            final BSONObject obj = decoder.readObject(data);
            System.out.println(obj.get("id"));
        }
    }
*/

/*
    private final static byte[] bsonDataSize = new byte[4];

    private static byte[] readNextBSONRawData(InputStream input) throws IOException {
        int readBytes = input.read(bsonDataSize, 0, 4);
        if (readBytes != 4) throw new EOFException();

        final int bsonSize = (bsonDataSize[0] & 0xff) |
                ((bsonDataSize[1] & 0xff) << 8) |
                ((bsonDataSize[2] & 0xff) << 16) |
                ((bsonDataSize[3] & 0xff) << 24);

        //System.out.println("bsonSize = "+bsonSize);

        final byte[] rawData = new byte[bsonSize];

        System.arraycopy(bsonDataSize, 0, rawData, 0, 4);

        for (int i = bsonSize - 4, off = 4; i > 0; off += readBytes) {
            readBytes = input.read(rawData, off, i);
            if (readBytes < 0) {
                throw new EOFException();
            }

            i -= readBytes;
        }

        return rawData;
    }
*/
}
