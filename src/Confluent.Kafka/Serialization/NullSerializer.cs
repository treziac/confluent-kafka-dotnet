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

namespace Confluent.Kafka.Serialization
{
    /// <summary>
    ///     A dummy serializer for use with values that must be null (the <see cref="Null"/> class cannot be instantiated).
    /// </summary>
    public class NullSerializer : ISerializer<Null>
    {
        /// <param name="val">
        ///     Can only be null (the <see cref="Null"/> class cannot be instantiated).
        /// </param>
        /// <returns>
        ///     null
        /// </returns>
        public byte[] Serialize(Null val)
        {
            return null;
        }

        byte[] ISerializer<Null>.Serialize(string topic, Null data)
        {
            return null;
        }
    }
}
