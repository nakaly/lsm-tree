package `LSM-KVS`

import java.io.RandomAccessFile

interface SegmentFileReadable {

    val readOnlyRaf: RandomAccessFile
    val RAF_LENGTH: Long
        get() = readOnlyRaf.length()

    fun hasNext(): Boolean {
        return readOnlyRaf.filePointer < RAF_LENGTH
    }

    fun currentPointer(): Long {
        return readOnlyRaf.filePointer
    }

    fun readKey(): String {
        return String(readKeyBytes())
    }

    fun readKeyBytes(): ByteArray {
        val keyLen = readOnlyRaf.readInt()
        val currentKey = ByteArray(keyLen)
        readOnlyRaf.readFully(currentKey)
        return currentKey
    }

    fun skipKey() {
        val keyLen = readOnlyRaf.readInt()
        readOnlyRaf.skipBytes(keyLen)
    }

    fun readValue(): Value {
        val valueLen = readOnlyRaf.readInt()
        if (valueLen != SegmentFile.TOMBSTONE) {
            val value = ByteArray(valueLen)
            readOnlyRaf.readFully(value)
            return Value.Exist(String(value))
        } else {
            return Value.Deleted
        }
    }

    fun skipValue() {
        val valueLen = readOnlyRaf.readInt()
        if (valueLen != SegmentFile.TOMBSTONE) {
            readOnlyRaf.skipBytes(valueLen)
        }
    }

    fun close() {
        readOnlyRaf.close()
    }

    sealed class Got {
        data class Found(val value: String) : Got()
        object NotFound : Got()
        object Deleted : Got()
    }


    sealed class Value {
        data class Exist(val value: String) : Value() {
            fun toGot(): Got.Found {
                return Got.Found(value)
            }
        }

        object Deleted : Value() {
            fun toGot(): Got.Deleted = Got.Deleted
        }
    }


}