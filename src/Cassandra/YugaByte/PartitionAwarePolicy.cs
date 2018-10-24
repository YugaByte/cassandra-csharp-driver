
// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using Cassandra.Serialization;

namespace Cassandra.YugaByte
{
    public class PartitionAwarePolicy : ILoadBalancingPolicy
    {
        private readonly ILoadBalancingPolicy _childPolicy = new DCAwareRoundRobinPolicy(null, int.MaxValue);
        private ICluster _cluster;

        public void Initialize(ICluster cluster)
        {
            _childPolicy.Initialize(cluster);
            _cluster = cluster;
        }

        public HostDistance Distance(Host host)
        {
            return _childPolicy.Distance(host);
        }

        public IEnumerable<Host> NewQueryPlan(string keyspace, IStatement query)
        {
            IEnumerable<Host> result = null;
            var boundStatement = query as BoundStatement;
            if (boundStatement != null)
            {
                result = NewQueryPlanImpl(keyspace, boundStatement);
            }
            else
            {
                var batchStatement = query as BatchStatement;
                if (batchStatement != null)
                {
                    result = NewQueryPlanImpl(keyspace, batchStatement);
                }
            }
            if (result == null)
            {
                result = _childPolicy.NewQueryPlan(keyspace, query);
            }
            return result;
        }

        private IEnumerable<Host> NewQueryPlanImpl(string keyspace, BatchStatement batchStatement)
        {
            foreach (var query in batchStatement.Queries)
            {
                var boundStatement = query as BoundStatement;
                if (boundStatement != null)
                {
                    var plan = NewQueryPlanImpl(keyspace, boundStatement);
                    if (plan != null)
                        return plan;
                }
            }
            return null;
        }

        private IEnumerable<Host> NewQueryPlanImpl(string keyspace, BoundStatement boundStatement)
        {
            var pstmt = boundStatement.PreparedStatement;
            var query = pstmt.Cql;
            var variables = pstmt.Variables;

            // Look up the hosts for the partition key. Skip statements that do not have bind variables.
            if (variables.Columns.Length == 0)
            {
                return null;
            }
            int key = GetKey(boundStatement);
            if (key < 0)
            {
                return null;
            }

            var fullTableName = variables.Keyspace + "." + variables.Columns[0].Table;
            TableSplitMetadata tableSplitMetadata;
            if (!_cluster.Metadata.TableSplitMetadata.TryGetValue(fullTableName, out tableSplitMetadata))
            {
                return null;
            }

            var hosts = Enumerable.ToArray(tableSplitMetadata.GetHosts(key));
            var consistencyLevel = boundStatement.ConsistencyLevel ?? _cluster.Configuration.QueryOptions.GetConsistencyLevel();
            if (consistencyLevel == ConsistencyLevel.YbConsistentPrefix)
            {
                Shuffle(hosts);
            }
            var strongConsistency = consistencyLevel.IsStrong();
            return IterateUpHosts(keyspace, hosts, strongConsistency);
        }

        private IEnumerable<Host> IterateUpHosts(string keyspace, Host[] hosts, bool strongConsistency)
        {
            foreach (var host in hosts)
            {
                if (host.IsUp && (strongConsistency || _childPolicy.Distance(host) == HostDistance.Local))
                {
                    yield return host;
                }
            }

            foreach (var host in _childPolicy.NewQueryPlan(keyspace, null))
            {
                if (Array.IndexOf(hosts, host) == -1 && (strongConsistency || _childPolicy.Distance(host) == HostDistance.Local))
                {
                    yield return host;
                }
            }
        }

        private static void Shuffle<T>(T[] array) {
            var rng = new Random();
            int n = array.Length;
            while (n > 1)
            {
                int k = rng.Next(n--);
                T temp = array[n];
                array[n] = array[k];
                array[k] = temp;
            }
        }

        public static int GetKey(BoundStatement boundStatement)
        {
            PreparedStatement pstmt = boundStatement.PreparedStatement;
            var hashIndexes = pstmt.RoutingIndexes;

            if (hashIndexes == null || hashIndexes.Length == 0)
            {
                return -1;
            }

            try
            {
                // Compute the hash key bytes, i.e. <h1><h2>...<h...>.
                var variables = pstmt.Variables;
                var values = boundStatement.QueryValues;
                using (var stream = new MemoryStream())
                using (var writer = new BinaryWriter(stream))
                {
                    foreach (var index in hashIndexes)
                    {
                        var type = variables.Columns[index].TypeCode;
                        var value = values[index];
                        WriteTypedValue(type, value, writer);
                    }
                    return BytesToKey(stream.ToArray());
                }
            } catch (InvalidCastException)
            {
                // We don't support cases when type of bound value does not match column type.
                return -1;
            }
        }

        public static long YBHashCode(BoundStatement boundStatement)
        {
            var hash = GetKey(boundStatement);
            if (hash == -1)
            {
                return -1;
            }
            return (long)(hash ^ 0x8000) << 48;
        }

        private static int BytesToKey(byte[] bytes)
        {
            ulong Seed = 97;
            ulong h = Jenkins.Hash64(bytes, Seed);
            ulong h1 = h >> 48;
            ulong h2 = 3 * (h >> 32);
            ulong h3 = 5 * (h >> 16);
            ulong h4 = 7 * (h & 0xffff);
            int result = (int)((h1 ^ h2 ^ h3 ^ h4) & 0xffff);
            // Console.WriteLine("Bytes to key {0}: {1}", BitConverter.ToString(bytes).Replace("-", ""), result);
            return result;
        }

        public static string ByteArrayToString(byte[] ba)
        {
            return BitConverter.ToString(ba).Replace("-", "");
        }

        private static void WriteTypedValue(ColumnTypeCode type, object value, BinaryWriter writer)
        {
            switch (type)
            {
                case ColumnTypeCode.Ascii:
                    writer.Write(Encoding.ASCII.GetBytes((string)value));
                    break;
                case ColumnTypeCode.Varchar:
                case ColumnTypeCode.Text:
                    writer.Write(Encoding.UTF8.GetBytes((string)value));
                    break;
                case ColumnTypeCode.Bigint:
                    writer.Write(IPAddress.HostToNetworkOrder((long)value));
                    break;
                case ColumnTypeCode.Blob:
                    writer.Write((byte[])value);
                    break;
                case ColumnTypeCode.Boolean:
                    writer.Write((bool)value);
                    break;
                case ColumnTypeCode.Double:
                    writer.Write(IPAddress.HostToNetworkOrder(BitConverter.DoubleToInt64Bits((double)value)));
                    break;
                case ColumnTypeCode.Float:
                    var bytes = BitConverter.GetBytes((float)value);
                    Array.Reverse(bytes);
                    writer.Write(bytes);
                    break;
                case ColumnTypeCode.Int:
                    writer.Write(IPAddress.HostToNetworkOrder((int)value));
                    break;
                case ColumnTypeCode.Timestamp:
                    // We should multiply by 1000 after division, because passing timestamp from Cassandra to YugaByte would also
                    // loose microsecond precision.
                    var milliseconds = ((DateTimeOffset)value - TypeSerializer.UnixStart).Ticks / TimeSpan.TicksPerMillisecond * 1000;
                    writer.Write(IPAddress.HostToNetworkOrder(milliseconds));
                    break;
                case ColumnTypeCode.Uuid:
                    writer.Write(TypeSerializer.GuidShuffle(((Guid)value).ToByteArray()));
                    break;
                case ColumnTypeCode.Timeuuid:
                    writer.Write(TypeSerializer.GuidShuffle(((TimeUuid)value).ToByteArray()));
                    break;
                case ColumnTypeCode.Inet:
                    writer.Write(((IPAddress)value).GetAddressBytes());
                    break;
                case ColumnTypeCode.Date:
                    writer.Write(TypeSerializer.PrimitiveLocalDateSerializer.Serialize(0, (LocalDate)value));
                    break;
                case ColumnTypeCode.Time:
                    writer.Write(IPAddress.HostToNetworkOrder(((LocalTime)value).TotalNanoseconds));
                    break;
                case ColumnTypeCode.SmallInt:
                    writer.Write(IPAddress.HostToNetworkOrder((short)value));
                    break;
                case ColumnTypeCode.TinyInt:
                    writer.Write((sbyte)value);
                    break;
                // Udt
                case ColumnTypeCode.Counter:
                case ColumnTypeCode.Custom:
                case ColumnTypeCode.Decimal:
                case ColumnTypeCode.Tuple:
                case ColumnTypeCode.Varint:
                case ColumnTypeCode.List:
                case ColumnTypeCode.Map:
                case ColumnTypeCode.Set:
                    throw new InvalidTypeException("Datatype " + type.ToString() + " not supported in a partition key column");
                default:
                    Console.WriteLine(string.Format("type={0}, value={1}, value.type={2}", type, value, value.GetType()));
                    break;
            }
        }

        public bool RequiresPartitionMap
        {
            get
            {
                return true;
            }
        }
    }
}
