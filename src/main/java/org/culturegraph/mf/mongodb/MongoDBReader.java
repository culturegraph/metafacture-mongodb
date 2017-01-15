/*
 *  Copyright 2013, 2014 Deutsche Nationalbibliothek
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.culturegraph.mf.mongodb;

import java.net.UnknownHostException;

import org.culturegraph.mf.framework.ObjectPipe;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.culturegraph.mf.mongodb.common.MongoDBKeys;
import org.culturegraph.mf.mongodb.common.SimpleMongoDBConnection;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Reads records from a MongoDB collection and transform them into a metadata
 * stream.
 * <p>
 * MongoDBReader supports a simple query syntax to select records. Queries are
 * given as input strings with one query per line.
 * <p>
 * The query syntax is
 *
 * <pre>
 * [field:]value
 * </pre>
 *
 * A field is addressed as the literal name prefixed with the concatenation of
 * entity names starting from the root entity. If the field is
 * {@link MongoDBKeys#RECORD_ID_KEY} or is omitted, the record id will be
 * searched. Note that both entity and literal names must be prefixed with
 * {@link MongoDBKeys#KEY_PREFIX}.
 *
 * @see MongoDBWriter
 * @author Thomas Seidel
 */
@Description("reads single-line queries to retrieve records from a MongoDB collection. "
		+ "Provide MongoDB access URI in brackets. "
		+ "URI syntax: monogdb://user:pass@host:port/database.collection?options...")
@In(String.class)
@Out(StreamReceiver.class)
public class MongoDBReader implements ObjectPipe<String, StreamReceiver> {

	private final MongoDBConnection mongoDBConnection;

	private StreamReceiver streamReceiver;

	/**
	 * Creates an instance of {@code MongoDBReader}.
	 *
	 * @param uri {@code monogdb://user:pass@host:port/database.collection?options...}
	 * @throws UnknownHostException if the IP address of the MongoDB server could
	 * not be determined.
	 */
	public MongoDBReader(final String uri) throws UnknownHostException {
		mongoDBConnection = new SimpleMongoDBConnection(uri);
	}

	public MongoDBReader(final MongoDBConnection mongoDBConnection) {
		this.mongoDBConnection = mongoDBConnection;
	}

	@Override
	public final void process(final String obj) {
		final DBObject dbQuery = parseQuery(obj);
		final DBCursor dbCursor = mongoDBConnection.find(dbQuery);
		while (dbCursor.hasNext()) {
			final DBObject dbObject = dbCursor.next();
			streamReceiver.startRecord((String) dbObject
					.get(MongoDBKeys.RECORD_ID_KEY));
			processBasicDBList((BasicDBList) dbObject.get(MongoDBKeys.DATA_KEY));
			streamReceiver.endRecord();
		}
	}

	private DBObject parseQuery(final String query) {
		final String[] tokens = query.split(":", 2);
		final DBObject dbQuery = new BasicDBObject();
		if (tokens.length == 1) {
			dbQuery.put(MongoDBKeys.RECORD_ID_KEY, tokens[0]);
		} else {
			dbQuery.put(MongoDBKeys.DATA_KEY + "." + tokens[0], tokens[1]);
		}
		return dbQuery;
	}

	private void processBasicDBList(final BasicDBList basicDBList) {
		for (final Object object : basicDBList) {
			final DBObject dbObject = (DBObject) object;
			for (final String key : dbObject.keySet()) {
				final Object value = dbObject.get(key);
				if (value instanceof BasicDBList) {
					streamReceiver.startEntity(key
							.substring(MongoDBKeys.KEY_PREFIX.length()));
					processBasicDBList((BasicDBList) value);
					streamReceiver.endEntity();
				} else {
					streamReceiver.literal(
							key.substring(MongoDBKeys.KEY_PREFIX.length()),
							(String) value);
				}
			}
		}
	}

	@Override
	public final void resetStream() {
		streamReceiver.resetStream();
	}

	@Override
	public final void closeStream() {
		streamReceiver.closeStream();
		mongoDBConnection.close();
	}

	public final <R extends StreamReceiver> R setReceiver(final R receiver) {
		streamReceiver = receiver;
		return receiver;
	}

}
