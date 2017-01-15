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
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.culturegraph.mf.framework.MetafactureException;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.culturegraph.mf.mongodb.common.MongoDBKeys;
import org.culturegraph.mf.mongodb.common.SimpleMongoDBConnection;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;

/**
 * Receives a metadata stream and writes the records into a MongoDB collection.
 * <p>
 * Format: One MongoDB document represents one record. The MongoDB document
 * identifier will be set to the record identifier. If the record identifier is
 * null, MongoDB chooses an unique document identifier. Entities are stored as
 * name/value pairs, value is a list which hold subsequent entities and
 * literals. Literals are stored as name/value pairs. Both entity and literal
 * keys will be prefixed with {@link MongoDBKeys#KEY_PREFIX}.
 * <p>
 * Example:
 * <p>
 * The metadata stream
 *
 * <pre>
 * startRecord(&quot;42&quot;);
 * literal(&quot;a&quot;, &quot;value1&quot;);
 * startEntity(&quot;A&quot;);
 * startEntity(&quot;B&quot;);
 * literal(&quot;b&quot;, &quot;value2&quot;);
 * endEntity();
 * literal(&quot;a&quot;, &quot;value3&quot;);
 * endEntity();
 * endRecord();
 * </pre>
 *
 * will be stored as
 *
 * <pre>
 * { "_id" : "42",
 *   "data" : [
 *     { "#a" : "value1" },
 *     { "#A" : [
 *       { "#B" : [
 *         { "#b" : "value2" }
 *         ]
 *       },
 *       { "#a" : "value3" }
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 *
 * @see MongoDBKeys
 * @see MongoDBReader
 * @author Thomas Seidel
 */
@Description("writes a stream into a MongoDB collection. "
		+ "Provide MongoDB access URI in brackets. "
		+ "URI syntax: monogdb://user:pass@host:port/database.collection?options...")
@In(StreamReceiver.class)
public class MongoDBWriter implements StreamReceiver {

	private final MongoDBConnection mongoDBConnection;

	private DBObject recordDBObject;
	private final Deque<List<DBObject>> dataStack = new LinkedList<>();

	/**
	 * Create an instance of {@code MongoDBWriter}.
	 *
	 * @param uri {@code monogdb://user:pass@host:port/database.collection?options...}
	 * @throws UnknownHostException if the IP address of the MongoDB server could
	 * not be determined.
	 */
	public MongoDBWriter(final String uri) throws UnknownHostException {
		mongoDBConnection = new SimpleMongoDBConnection(uri);
	}

	public MongoDBWriter(final MongoDBConnection mongoDBConnection) {
		this.mongoDBConnection = mongoDBConnection;
	}

	@Override
	public final void startRecord(final String identifier) {
		dataStack.clear();
		recordDBObject = new BasicDBObject();
		if (identifier != null) {
			recordDBObject.put(MongoDBKeys.RECORD_ID_KEY, identifier);
		}
		final List<DBObject> dbObjectList = new ArrayList<>();
		recordDBObject.put(MongoDBKeys.DATA_KEY, dbObjectList);
		dataStack.push(dbObjectList);
	}

	@Override
	public final void startEntity(final String identifier) {
		final DBObject entityDBObject = new BasicDBObject();
		final List<DBObject> dbObjectList = new ArrayList<>();
		entityDBObject.put(MongoDBKeys.KEY_PREFIX + identifier, dbObjectList);
		dataStack.peek().add(entityDBObject);
		dataStack.push(dbObjectList);
	}

	@Override
	public final void literal(final String identifier, final String value) {
		final DBObject literalDBObject = new BasicDBObject();
		literalDBObject.put(MongoDBKeys.KEY_PREFIX + identifier, value);
		dataStack.peek().add(literalDBObject);
	}

	@Override
	public final void endEntity() {
		dataStack.pop();
	}

	@Override
	public final void endRecord() {
		try {
			mongoDBConnection.save(recordDBObject);
		} catch (final MongoException mongoException) {
			throw new MetafactureException(mongoException);
		} finally {
			dataStack.clear();
		}
	}

	@Override
	public final void resetStream() {
		dataStack.clear();
	}

	@Override
	public final void closeStream() {
		dataStack.clear();
		mongoDBConnection.close();
	}

}
