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

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.mongodb.MongoDBReader;
import org.culturegraph.mf.mongodb.common.MongoDBConnection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 *
 * @author Thomas Seidel
 *
 */
public final class MongoDBReaderTest {

	private static final String QUERY_DBOBJECT_AS_JSON = "{ \"_id\" : \"23\" }";

	private static final String RETRIEVED_DBOBJECT_AS_JSON = "{ \"_id\" : \"23\","
			+ "   \"data\" : ["
			+ "     { \"#c\" : \"value1\" },"
			+ "     { \"#C\" : ["
			+ "       { \"#D\" : ["
			+ "         { \"#d\" : \"value2\" }"
			+ "         ]"
			+ "       },"
			+ "       { \"#c\" : \"value3\" }"
			+ "       ]"
			+ "     }"
			+ "   ]"
			+ " }";

	@Rule
	public MockitoRule mockitoRule = MockitoJUnit.rule();

	private MongoDBReader mongoDBReader;

	@Mock
	private StreamReceiver receiver;

	@Mock
	private MongoDBConnection mongoDBConnection;

	@Mock
	private DBCursor dbCursor;

	@Before
	public void setup() {
		mongoDBReader = new MongoDBReader(mongoDBConnection);
		mongoDBReader.setReceiver(receiver);
	}

	@Test
	public void shouldRetrieveSingleDBObjectAsStream() {
		final DBObject queryDBObject = (DBObject) JSON.parse(
				QUERY_DBOBJECT_AS_JSON);
		final DBObject retrievedDBObject = (DBObject) JSON.parse(
				RETRIEVED_DBOBJECT_AS_JSON);
		when(dbCursor.hasNext()).thenReturn(true, false);
		when(dbCursor.next()).thenReturn(retrievedDBObject);
		when(mongoDBConnection.find(queryDBObject)).thenReturn(dbCursor);

		mongoDBReader.process("23");
		mongoDBReader.closeStream();

		final InOrder ordered = inOrder(receiver);
		ordered.verify(receiver).startRecord("23");
		ordered.verify(receiver).literal("c", "value1");
		ordered.verify(receiver).startEntity("C");
		ordered.verify(receiver).startEntity("D");
		ordered.verify(receiver).literal("d", "value2");
		ordered.verify(receiver).endEntity();
		ordered.verify(receiver).literal("c", "value3");
		ordered.verify(receiver).endEntity();
		ordered.verify(receiver).endRecord();
	}

}
