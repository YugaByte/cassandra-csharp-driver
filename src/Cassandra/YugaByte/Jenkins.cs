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

/**
* A hash function that returns a 64-bit hash value.
*
* Based on Bob Jenkins' new hash function (http://burtleburtle.net/bob/hash/evahash.html)
*/
public class Jenkins
{
    /// <summary>
    /// Returns a 64-bit hash value of a byte sequence.
    /// </summary>
    /// <param name="k">the byte sequence</param>
    /// <param name="initval">the initial (seed) value</param>
    /// <returns>the 64-bit hash value</returns>
    public static ulong Hash64(byte[] k, ulong initval)
    {
        // Set up the internal state
        ulong a = 0xe08c1d668b756f82UL; // the golden ratio; an arbitrary value
        ulong b = 0xe08c1d668b756f82UL;
        ulong c = initval;             // variable initialization of internal state
        int pos = 0;
        // handle most of the key
        while (k.Length - pos >= 24)
        {
            a += GetLong(k, pos); pos += 8;
            b += GetLong(k, pos); pos += 8;
            c += GetLong(k, pos); pos += 8;
            // mix64(a, b, c);
            a -= b; a -= c; a ^= (c >> 43);
            b -= c; b -= a; b ^= (a << 9);
            c -= a; c -= b; c ^= (b >> 8);
            a -= b; a -= c; a ^= (c >> 38);
            b -= c; b -= a; b ^= (a << 23);
            c -= a; c -= b; c ^= (b >> 5);
            a -= b; a -= c; a ^= (c >> 35);
            b -= c; b -= a; b ^= (a << 49);
            c -= a; c -= b; c ^= (b >> 11);
            a -= b; a -= c; a ^= (c >> 12);
            b -= c; b -= a; b ^= (a << 18);
            c -= a; c -= b; c ^= (b >> 22);
        }
        // handle the last 23 bytes
        c += (ulong)k.Length;
        switch (k.Length - pos)
        { // all the case statements fall through
            case 23: c += GetByte(k, pos + 22) << 56; goto case 22;
            case 22: c += GetByte(k, pos + 21) << 48; goto case 21;
            case 21: c += GetByte(k, pos + 20) << 40; goto case 20;
            case 20: c += GetByte(k, pos + 19) << 32; goto case 19;
            case 19: c += GetByte(k, pos + 18) << 24; goto case 18;
            case 18: c += GetByte(k, pos + 17) << 16; goto case 17;
            case 17: c += GetByte(k, pos + 16) << 8; goto case 16;
            // the first byte of c is reserved for the length
            case 16: b += GetLong(k, pos + 8); a += GetLong(k, pos); break; // special handling
            case 15: b += GetByte(k, pos + 14) << 48; goto case 14;
            case 14: b += GetByte(k, pos + 13) << 40; goto case 13;
            case 13: b += GetByte(k, pos + 12) << 32; goto case 12;
            case 12: b += GetByte(k, pos + 11) << 24; goto case 11;
            case 11: b += GetByte(k, pos + 10) << 16; goto case 10;
            case 10: b += GetByte(k, pos + 9) << 8; goto case 9;
            case 9: b += GetByte(k, pos + 8); goto case 8;
            case 8: a += GetLong(k, pos); break; // special handling
            case 7: a += GetByte(k, pos + 6) << 48; goto case 6;
            case 6: a += GetByte(k, pos + 5) << 40; goto case 5;
            case 5: a += GetByte(k, pos + 4) << 32; goto case 4;
            case 4: a += GetByte(k, pos + 3) << 24; goto case 3;
            case 3: a += GetByte(k, pos + 2) << 16; goto case 2;
            case 2: a += GetByte(k, pos + 1) << 8; goto case 1;
            case 1: a += GetByte(k, pos); break;
                // case 0: nothing left to add
        }
        // mix64(a, b, c);
        a -= b; a -= c; a ^= (c >> 43);
        b -= c; b -= a; b ^= (a << 9);
        c -= a; c -= b; c ^= (b >> 8);
        a -= b; a -= c; a ^= (c >> 38);
        b -= c; b -= a; b ^= (a << 23);
        c -= a; c -= b; c ^= (b >> 5);
        a -= b; a -= c; a ^= (c >> 35);
        b -= c; b -= a; b ^= (a << 49);
        c -= a; c -= b; c ^= (b >> 11);
        a -= b; a -= c; a ^= (c >> 12);
        b -= c; b -= a; b ^= (a << 18);
        c -= a; c -= b; c ^= (b >> 22);
        return c;
    }
    /**
    * Retrieves a byte in a byte array as a long.
    *
    * @param k    the byte array
    * @param pos  the byte position
    * @return     the byte as long
    */
    private static ulong GetByte(byte[] k, int pos)
    {
        return k[pos];
    }
    /**
    * Retrieves a long in a byte array in little-endian order.
    *
    * @param k    the byte array
    * @param pos  the long position
    * @return     the long in little-endian order
    */
    private static ulong GetLong(byte[] k, int pos)
    {
        return (GetByte(k, pos) |
                GetByte(k, pos + 1) << 8 |
                GetByte(k, pos + 2) << 16 |
                GetByte(k, pos + 3) << 24 |
                GetByte(k, pos + 4) << 32 |
                GetByte(k, pos + 5) << 40 |
                GetByte(k, pos + 6) << 48 |
                GetByte(k, pos + 7) << 56);
    }
}