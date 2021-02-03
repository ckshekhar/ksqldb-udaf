/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.example.ksql.functions;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;


/**
 * In this example, we implement a UDAF for computing some summary statistics for a stream
 * of doubles.
 *
 * <p>Example query usage:
 *
 * <pre>{@code
 * CREATE STREAM api_responses (username VARCHAR, response_code INT, response_time DOUBLE) \
 * WITH (kafka_topic='api_logs', value_format='JSON');
 *
 * SELECT username, SUMMARY_STATS(response_time) \
 * FROM api_responses \
 * GROUP BY username ;
 * }</pre>
 */
@UdafDescription(
        name = "chatroom_list",
        description = "Example UDAF that computes some summary stats for a stream of doubles",
        version = "0.1.0-SNAPSHOT",
        author = "Shekhar"
)
public final class ListChatroom{

    static final Schema CH_STRUCT = SchemaBuilder.struct().optional()
                .field("ID", Schema.INT32_SCHEMA)
                .field("TITLE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("OWNER", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CREATOR", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

    static final Schema ch_list = SchemaBuilder.array(CH_STRUCT).build();

    static final String PARAM_SCHEMA_DESCRIPTOR = "STRUCT<" +
            "ID int," +
            "TITLE String," +
            "OWNER String," +
            "CREATOR String" +
            ">";
    static final  String AGG_SCHEMA_DESCRIPTOR = "ARRAY<STRUCT<" +
            "ID int," +
                    "TITLE String," +
                    "OWNER String," +
                    "CREATOR String" +
                    ">>";

    @UdafFactory(description = "compute summary stats for doubles", paramSchema=PARAM_SCHEMA_DESCRIPTOR,
    aggregateSchema = AGG_SCHEMA_DESCRIPTOR, returnSchema = AGG_SCHEMA_DESCRIPTOR)
    // Can be used with stream aggregations. The input of our aggregation will be doubles,
    // and the output will be a map
    public static Udaf<Struct, List<Struct>, List<Struct>> listChatroom() {

        return new Udaf<Struct, List<Struct>, List<Struct>>() {

            private List<Struct> aggOne;

            /**
             * Specify an initial value for our aggregation
             *
             * @return the initial state of the aggregate.
             */
            @Override
            public List<Struct> initialize() {
                return new ArrayList<Struct>();
            }

            /**
             * Perform the aggregation whenever a new record appears in our stream.
             *
             * @param newValue the new value to add to the {@code aggregateValue}.
             * @param aggregateValue the current aggregate.
             * @return the new aggregate value.
             */
            @Override
            public List<Struct> aggregate(
                    final Struct newValue,
                    final List<Struct> aggregateValue
            ) {
                if (newValue == null) {
                    return aggregateValue;
                }
                // calculate the new aggregate
                final Struct ch = new Struct(CH_STRUCT).put("ID", newValue.getInt32("ID"))
                        .put("TITLE", newValue.get("TITLE"))
                        .put("OWNER", newValue.get("OWNER"))
                        .put("CREATOR", newValue.get("CREATOR"));
                aggregateValue.add(ch);
                return aggregateValue;
            }

            @Override
            public List<Struct> map(final List<Struct> aggregate) {
                return  aggregate;
            }

            /**
             * Called to merge two aggregates together.
             *
             * @param aggOne the first aggregate
             * @param aggTwo the second aggregate
             * @return the merged result
             */
            @Override
            public List<Struct> merge(
                    final List<Struct> aggOne,
                    final List<Struct> aggTwo
            ) {
                aggOne.addAll(aggTwo);
                return aggOne;
            }
        };
    }
}
