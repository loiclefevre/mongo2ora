package org.bson;

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

/*
    public static void main(String[] args) {

        String j="{\"_id\": {\"$oid\": \"61685df979fb4978a7be0a12\"}, \"test\": {\"_id\":{\"$oid\":\"61685df979fb4978a7be0a13\"}}, \"movie_id\": 1, \"sku\": \"COO3790\", \"list_price\": 3.99, \"wiki_article\": \"'Gator_Bait_II:_Cajun_Justice\", \"title\": \"'Gator Bait II: Cajun Justice\", \"opening_date\": \"1988-01-01\", \"year\": 1988, \"views\": 6, \"cast\": null, \"crew\": [{\"job\": \"director\", \"names\": [\"Beverly Sebastion\"]}, {\"job\": \"screenwriter\", \"names\": [\"Beverly Sebastion\"]}], \"studio\": null, \"runtime\": 95, \"budget\": null, \"gross\": null, \"genre\": [\"Thriller\"], \"main_subject\": null, \"awards\": null, \"nominations\": null, \"image_url\": \"https://upload.wikimedia.org/wikipedia/en/9/91/Gatorbait2.jpg\", \"summary\": \"'  Gator Bait II: Cajun Justice is a 1988 sequel to the 1974 film 'Gator Bait , written, produced and directed by Beverly Sebastian and Ferd Sebastian. Largely ignored upon release, the film received a second life on cable television and home video.\"}";
        new MyBSONDecoder(true).convertBSONToOSON(RawBsonDocument.parse(j));
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
