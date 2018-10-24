using System;
using System.Text;

namespace Cassandra.Serialization.Primitive
{
    internal class JsonSerializer : TypeSerializer<string>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Json; }
        }

        public override string Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            return Encoding.UTF8.GetString(buffer, offset, length);
        }

        public override byte[] Serialize(ushort protocolVersion, string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }
    }
}
