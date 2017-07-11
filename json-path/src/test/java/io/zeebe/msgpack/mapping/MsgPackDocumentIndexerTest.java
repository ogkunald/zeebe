/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.msgpack.mapping;

import static io.zeebe.msgpack.mapping.MappingTestUtil.*;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class MsgPackDocumentIndexerTest
{
    private final MsgPackDocumentIndexer indexer = new MsgPackDocumentIndexer();

    @Test
    public void shouldIndexDocument() throws Exception
    {
        // given document
        final DirectBuffer document = new UnsafeBuffer(MSG_PACK_BYTES);
        indexer.wrap(document);

        // when
        final MsgPackTree documentTree = indexer.index();

        // then tree is expected as
        assertThatIsMapNode(documentTree, "$", NODE_STRING_KEY, NODE_BOOLEAN_KEY, NODE_INTEGER_KEY,
                                                                   NODE_LONG_KEY, NODE_DOUBLE_KEY, NODE_ARRAY_KEY,
                                                                   NODE_JSON_OBJECT_KEY);
        assertThatIsMapNode(documentTree, "$.jsonObject", NODE_TEST_ATTR_KEY);
        assertThatIsArrayNode(documentTree, "$.array", "0", "1", "2", "3");

        assertThatIsLeafNode(documentTree, "$.string", MSGPACK_MAPPER.writeValueAsBytes(NODE_STRING_VALUE));
        assertThatIsLeafNode(documentTree, "$.boolean", MSGPACK_MAPPER.writeValueAsBytes(NODE_BOOLEAN_VALUE));
        assertThatIsLeafNode(documentTree, "$.integer", MSGPACK_MAPPER.writeValueAsBytes(NODE_INTEGER_VALUE));
        assertThatIsLeafNode(documentTree, "$.long", MSGPACK_MAPPER.writeValueAsBytes(NODE_LONG_VALUE));
        assertThatIsLeafNode(documentTree, "$.double", MSGPACK_MAPPER.writeValueAsBytes(NODE_DOUBLE_VALUE));
        assertThatIsLeafNode(documentTree, "$.array.0", MSGPACK_MAPPER.writeValueAsBytes(0));
        assertThatIsLeafNode(documentTree, "$.array.1", MSGPACK_MAPPER.writeValueAsBytes(1));
        assertThatIsLeafNode(documentTree, "$.array.2", MSGPACK_MAPPER.writeValueAsBytes(2));
        assertThatIsLeafNode(documentTree, "$.array.3", MSGPACK_MAPPER.writeValueAsBytes(3));
        assertThatIsLeafNode(documentTree, "$.jsonObject.testAttr", MSGPACK_MAPPER.writeValueAsBytes(NODE_TEST_ATTR_VALUE));
    }

    @Test
    public void shouldIndexDocumentWithMoreArrays() throws Exception
    {
        // given document
        final String jsonDocument =
                "{'first': { 'range': [0, 2], 'friends': [-1, {'id': 0, 'name': 'Rodriguez Richards'}]," +
                "'greeting': 'Hello, Bauer! You have 7 unread messages.', 'favoriteFruit': 'apple'}}";
        final byte[] msgPackBytes = MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree(jsonDocument));
        final DirectBuffer document = new UnsafeBuffer(msgPackBytes);
        indexer.wrap(document);

        // when
        final MsgPackTree documentTree = indexer.index();

        // then tree is expected as
        assertThatIsMapNode(documentTree, "$", "first");
        assertThatIsMapNode(documentTree, "$.first", "range", "friends", "greeting", "favoriteFruit");

        assertThatIsArrayNode(documentTree, "$.first.range", "0", "1");
        assertThatIsLeafNode(documentTree, "$.first.range.0", MSGPACK_MAPPER.writeValueAsBytes(0));
        assertThatIsLeafNode(documentTree, "$.first.range.1", MSGPACK_MAPPER.writeValueAsBytes(2));

        assertThatIsArrayNode(documentTree, "$.first.friends", "0", "1");
        assertThatIsLeafNode(documentTree, "$.first.friends.0", MSGPACK_MAPPER.writeValueAsBytes(-1));
        assertThatIsMapNode(documentTree, "$.first.friends.1", "id", "name");
        assertThatIsLeafNode(documentTree, "$.first.friends.1.id", MSGPACK_MAPPER.writeValueAsBytes(0));
        assertThatIsLeafNode(documentTree, "$.first.friends.1.name", MSGPACK_MAPPER.writeValueAsBytes("Rodriguez Richards"));

        assertThatIsLeafNode(documentTree, "$.first.greeting", MSGPACK_MAPPER.writeValueAsBytes("Hello, Bauer! You have 7 unread messages."));
        assertThatIsLeafNode(documentTree, "$.first.favoriteFruit", MSGPACK_MAPPER.writeValueAsBytes("apple"));
    }

    @Test
    public void shouldIndexDocumentWitObjectArray() throws Exception
    {
        // given document
        final String jsonDocument =
                "{'friends': [{'id': 0, 'name': 'Rodriguez Richards'}, {'id': 0, 'name': 'Rodriguez Richards'}]}";
        final byte[] msgPackBytes = MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree(jsonDocument));
        final DirectBuffer document = new UnsafeBuffer(msgPackBytes);
        indexer.wrap(document);

        // when
        final MsgPackTree documentTree = indexer.index();

        // then tree is expected as
        assertThatIsMapNode(documentTree, "$", "friends");
        assertThatIsArrayNode(documentTree, "$.friends", "0", "1");

        assertThatIsMapNode(documentTree, "$.friends.0", "id", "name");
        assertThatIsLeafNode(documentTree, "$.friends.0.id", MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree("0")));
        assertThatIsLeafNode(documentTree, "$.friends.0.name", MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree("'Rodriguez Richards'")));

        assertThatIsMapNode(documentTree, "$.friends.1", "id", "name");
        assertThatIsLeafNode(documentTree, "$.friends.1.id", MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree("0")));
        assertThatIsLeafNode(documentTree, "$.friends.1.name", MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree("'Rodriguez Richards'")));
    }

    @Test
    public void shouldIndexDocumentWitArrayAndObjectWithIndex() throws Exception
    {
        // given document
        final String jsonDocument = "{'a':['foo'], 'a0':{'b':'c'}}";
        final byte[] msgPackBytes = MSGPACK_MAPPER.writeValueAsBytes(JSON_MAPPER.readTree(jsonDocument));
        final DirectBuffer document = new UnsafeBuffer(msgPackBytes);
        indexer.wrap(document);

        // when
        final MsgPackTree documentTree = indexer.index();

        // then tree is expected as
        assertThatIsMapNode(documentTree, "$", "a", "a0");

        assertThatIsArrayNode(documentTree, "$.a", "0");
        assertThatIsLeafNode(documentTree, "$.a.0", MSGPACK_MAPPER.writeValueAsBytes("foo"));

        assertThatIsMapNode(documentTree, "$.a0", "b");
        assertThatIsLeafNode(documentTree, "$.a0.b", MSGPACK_MAPPER.writeValueAsBytes("c"));
    }


}
