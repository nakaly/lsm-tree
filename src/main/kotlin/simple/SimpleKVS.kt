package simple

import com.github.michaelbull.result.Err
import com.github.michaelbull.result.Ok
import com.github.michaelbull.result.Result
import java.io.IOException
import java.io.RandomAccessFile



fun main() {
    val test = arrayListOf<Int>()
    test.add(2)
    test.add(5)
    test.add(8)
    test.add(9)
    test.add(14)
    test.add(17)
    println(test.binarySearch(2))
    println(test.binarySearch(4))
    println(test.binarySearch(7))
    println(test.binarySearch(8))
    println(test.binarySearch(16))
    println(test.binarySearch(20))
    println(test.binarySearch(1))
    println(test.binarySearch(0))
//    val raf = RandomAccessFile("data/simplekvs/database.txt", "rw")
//    raf.writeBytes("test")
//    raf.close()
//    val kvs = SimpleKVS("data/simplekvs/database.txt")
//    kvs.set("key", "1")
//    kvs.set("key2", "2")
//    val value = kvs.get("key")
//    println("value: $value")
//    val value2 = kvs.get("key2")
//    println("value: $value2")

}


class SimpleKVS(filePath: String) {
    private val raf = RandomAccessFile(filePath, "rw")
    private val keyIndex = mutableMapOf<String, Long>()

    private val KEY_HEADER_SIZE = Integer.BYTES
    private val VALUE_HEADER_SIZE = Integer.BYTES
    private val HEADER_SIZE = Integer.BYTES * 2
    private val MIN_DATA_SIZE = HEADER_SIZE + 2
    private val TOMBSTONE = Integer.MIN_VALUE

    fun set(key: String, value: String): Result<Unit, Exception> {
        val befPos = raf.length()
        try {
            raf.seek(raf.length())
            raf.writeBytes(value)
            raf.writeInt(value.length)
            raf.writeBytes(key)
            raf.writeInt(key.length)
        } catch (e: IOException) {
            raf.setLength(befPos)
            return Err(e)
        }
        keyIndex[key] = raf.filePointer
        return Ok(Unit)
    }

    private fun seek(key: ByteArray, befPos: Long): String? {
        if (befPos >= MIN_DATA_SIZE) {
            var pos = befPos
            raf.seek(pos)

            // Move pos to the position where key header starts
            pos -= KEY_HEADER_SIZE
            raf.seek(pos)
            // Read key header
            val keylen = raf.readInt()
            // Move pos to the position where key starts
            pos -= keylen
            raf.seek(pos)
            // Read key
            val currentKey = ByteArray(keylen)
            raf.readFully(currentKey)

            // Move pos to the position where value header starts
            pos -= VALUE_HEADER_SIZE
            raf.seek(pos)
            // Read value header
            val valueLen = raf.readInt()
            if (valueLen == TOMBSTONE) {
                return null
            }
            pos -= valueLen
            if (currentKey.contentEquals(key)) {
                val value = ByteArray(valueLen)
                raf.seek(pos)
                raf.readFully(value)
                return String(value)
            } else {
                return seek(key, pos)
            }
        } else {
            return null
        }
    }

    fun get(key: String): String? {
        return try {
            val pos = keyIndex.getOrDefault(key, raf.length())
            seek(key.toByteArray(), pos)
        } catch (e: IOException) {
            null
        }
    }

    fun del(key: String): Result<Unit, Exception> {
        return when (get(key)) {
            null -> Ok(Unit)
            else -> {
                val befPos = raf.length()
                try {
                    raf.seek(raf.length())
                    raf.writeInt(TOMBSTONE)
                    raf.writeBytes(key)
                    raf.writeInt(key.length)
                } catch (e: IOException) {
                    raf.setLength(befPos)
                    Err(e)
                }
                keyIndex[key] = raf.filePointer
                Ok(Unit)
            }
        }
    }

}

