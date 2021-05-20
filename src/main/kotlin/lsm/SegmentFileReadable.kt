package lsm

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
            return Exist(String(value))
        } else {
            return DeletedValue
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

    sealed interface Got
    data class Found(val value: String) : Got
    object NotFound : Got
    object Deleted : Got


    sealed interface Value {
        fun toGot(): Got
    }

    data class Exist(val value: String) : Value {
        override fun toGot(): Found {
            return Found(value)
        }
    }

    object DeletedValue : Value {
        override fun toGot(): Deleted = Deleted
    }


}