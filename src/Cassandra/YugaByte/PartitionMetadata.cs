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

using System.Collections.Generic;
using System.Linq;

namespace Cassandra.YugaByte
{
    /// <summary>
    /// The metadata for one table partition. It maintains the partition's range (start and end key) and
    /// hosts (leader and followers).
    /// </summary>
    public struct PartitionMetadata
    {
        // The partition start -- inclusive bound.
        public int StartKey { get; private set; }
        // The partition end -- exclusive bound.
        public int EndKey { get; private set; }
        // The list of hosts -- first one should be the leader, then the followers in no particular order.
        // TODO We should make the leader explicit here and in the return type of the getQueryPlan method
        // to be able to distinguish the case where the leader is missing so the hosts are all followers.
        public IEnumerable<Host> Hosts { get; private set; }

        public bool Valid {
            get
            {
                return StartKey >= 0;
            }
        }

        /// <summary>Creates a new PartitionMetadata</summary>
        public PartitionMetadata(int startKey, int endKey, IList<Host> hosts)
        {
            StartKey = startKey;
            EndKey = endKey;
            Hosts = (hosts != null) ? new List<Host>(hosts) : Enumerable.Empty<Host>();
        }

        public override string ToString()
        {
            return string.Format("[{0}, {1}) -> {2}", StartKey, EndKey, string.Join(", ", Hosts));
        }
    }
}
