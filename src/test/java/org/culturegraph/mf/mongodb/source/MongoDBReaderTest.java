/*
 *  Copyright 2013 Deutsche Nationalbibliothek
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

package org.culturegraph.mf.mongodb.source;

import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.culturegraph.mf.stream.sink.EventList;
import org.culturegraph.mf.stream.sink.StreamValidator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * 
 * @author Thomas Seidel
 * 
 */
public final class MongoDBReaderTest {

	private static final String QUERY_DBOBJECT_AS_JSON = "{ \"_id\" : \"42\" }";

	private static final String RETRIEVED_DBOBJECT_AS_JSON = "{ \"_id\" : \"42\","
			+ "   \"data\" : ["
			+ "     { \"#a\" : \"value1\" },"
			+ "     { \"#A\" : ["
			+ "       { \"#B\" : ["
			+ "         { \"#b\" : \"value2\" }"
			+ "         ]"
			+ "       },"
			+ "       { \"#a\" : \"value3\" }"
			+ "       ]"
			+ "     }"
			+ "   ]"
			+ " }";

	private MongoDBReader mongoDBReader;

	@Mock
	private MongoDBConnection mongoDBConnection;
	@Mock
	private DBCursor dbCursor;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		mongoDBReader = new MongoDBReader(mongoDBConnection);
	}

	@Test
	public void shouldRetrieveSingleDBObjectAsStream() {
		final DBObject queryDBObject = (DBObject) JSON
				.parse(QUERY_DBOBJECT_AS_JSON);
		final DBObject retrievedDBObject = (DBObject) JSON
				.parse(RETRIEVED_DBOBJECT_AS_JSON);
		Mockito.when(dbCursor.hasNext()).thenReturn(true, false);
		Mockito.when(dbCursor.next()).thenReturn(retrievedDBObject);
		Mockito.when(mongoDBConnection.find(queryDBObject))
				.thenReturn(dbCursor);

		final EventList expected = new EventList();
		expected.startRecord("42");
		expected.literal("a", "value1");
		expected.startEntity("A");
		expected.startEntity("B");
		expected.literal("b", "value2");
		expected.endEntity();
		expected.literal("a", "value3");
		expected.endEntity();
		expected.endRecord();
		expected.closeStream();
		final StreamValidator streamValidator = new StreamValidator(
				expected.getEvents());
		mongoDBReader.setReceiver(streamValidator);

		mongoDBReader.process("42");
		mongoDBReader.closeStream();
	}
}
