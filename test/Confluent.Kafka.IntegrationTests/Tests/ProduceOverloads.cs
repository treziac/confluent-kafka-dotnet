// Copyright 2016-2017 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Xunit;
using System.Threading.Tasks;


namespace Confluent.Kafka.IntegrationTests
{
    /// <summary>
    ///     Test every Producer.ProduceAsync method overload.
    /// </summary>
    public static partial class Tests
    {
        [Theory, MemberData(nameof(KafkaParameters))]
        public static void ProduceOverloads(string bootstrapServers, string topic)
        {
            var consumerConfig = new Dictionary<string, object>
            {
                { "group.id", "produce-overloads-cg" },
                { "bootstrap.servers", bootstrapServers },
                { "session.timeout.ms", 6000 }
            };

            var producerConfig = new Dictionary<string, object> { { "bootstrap.servers", bootstrapServers } };

            var testTimestamps = Library.Version >= 0x000904000;
            var testDate = new DateTime(2010, 1, 1, 1, 1, 1, DateTimeKind.Utc);

            var drs = new List<Task<Message<string, string>>>();
            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                drs.Add(producer.ProduceAsync(topic, "test key 0", "test val 0", 0, true));
                drs.Add(producer.ProduceAsync(topic, "test key 1", "test val 1", 0));
                drs.Add(producer.ProduceAsync(topic, "test key 2", "test val 2", true));
                drs.Add(producer.ProduceAsync(topic, "test key 3", "test val 3"));
                if (testTimestamps)
                {
                    drs.Add(producer.ProduceAsync(topic, "test key 4", "test val 4", testDate, 0, true));
                    drs.Add(producer.ProduceAsync(topic, "test key 5", "test val 5", testDate, 0));
                    drs.Add(producer.ProduceAsync(topic, "test key 6", "test val 6", testDate));
                }
                producer.Flush();
            }

            for (int i=0; i<7; ++i)
            {
                if (!testTimestamps && i > 3)
                {
                    break;
                }
                var dr = drs[i].Result;

                Assert.Equal(ErrorCode.NoError, dr.Error.Code);
                Assert.Equal(0, dr.Partition);
                Assert.Equal(topic, dr.Topic);
                Assert.NotEqual(Offset.Invalid, dr.Offset);
                Assert.Equal(dr.Key, $"test key {i}");
                Assert.Equal(dr.Value, $"test val {i}");
                if (i > 3)
                {
                    Assert.Equal(dr.Timestamp.DateTime, testDate);
                }
            }

            // unfinished.

/*
            using (var consumer = new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Subscribe(topic);
                Assert.Equal(consumer.Assignment.Count, 0);
                consumer.Poll(TimeSpan.FromSeconds(1));
                Assert.Equal(consumer.Assignment.Count, 1);
                Assert.Equal(consumer.Assignment[0].Topic, topic);
            }
*/

        }
    }
}
