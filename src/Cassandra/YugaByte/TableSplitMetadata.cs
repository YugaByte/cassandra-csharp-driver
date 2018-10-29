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
using System.Linq;

namespace Cassandra.YugaByte
{
    /**
    * The partition split for a table. It maintains a map from start key to partition metadata for each
    * partition split of the table.
    */
    public class TableSplitMetadata
    {
        // Map from start-key to partition metadata representing the partition split of a table.
        private readonly int[] _startKeys;
        private readonly PartitionMetadata[] _partitions;
        /// <summary>
        /// Creates a new {@code TableSplitMetadata}.
        /// </summary>
        public TableSplitMetadata(IEnumerable<PartitionMetadata> source)
        {
            _partitions = Enumerable.ToArray(source);
            Array.Sort(_partitions, (lhs, rhs) =>
            {
                return lhs.StartKey - rhs.StartKey;
            });
            _startKeys = new int[_partitions.Length];
            for (int i = 0; i != _partitions.Length; ++i)
            {
                _startKeys[i] = _partitions[i].StartKey;
            }
        }

        /// <summary>
        /// Returns the partition metadata for the partition key in the given table.
        /// </summary>
        /// <param name="key">the partition key</param>
        /// <returns>the partition metadata for the partition key, or {@code null} when there is no
        /// partition information available</returns>
        public PartitionMetadata? GetPartitionMetadata(int key)
        {
            int index = Array.BinarySearch(_startKeys, key);
            if (index < 0)
            {
                index = ~index - 1;
            }
            if (index < 0) // key less than minimal start key
            {
                return null;
            }
            return _partitions[index];
        }

        /// <summary>
        /// Returns the hosts for the partition key in the given table.
        /// </summary>
        /// <param name="key">the partition key</param>
        /// <returns>the hosts for the partition key, or an empty list when there is no hosts information available</returns>
        public IEnumerable<Host> GetHosts(int key)
        {
            return GetPartitionMetadata(key)?.Hosts;
        }
    }
}
