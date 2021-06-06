package lsm.sstable

import lsm.sstable.SegmentFile.Companion.TOMBSTONE
import java.io.RandomAccessFile
import java.util.*

class WriteAheadLog(val raf: RandomAccessFile) : SegmentFileReadable {

    override val readOnlyRaf: RandomAccessFile = raf

    fun set(key: String, value: String): Unit {
        raf.writeBytes(value)
        raf.writeInt(value.length)
        raf.writeBytes(key)
        raf.writeInt(key.length)
    }

    fun del(key: String): Unit {
        raf.writeInt(TOMBSTONE)
        raf.writeBytes(key)
        raf.writeInt(key.length)
    }

    fun recovery(): Log.MemTable {
        val treeMap = TreeMap<String, SegmentFileReadable.Value>()

        tailrec fun loop(): Unit {
            if (hasNext()) {
                val key = readKey()
                val value = readValue()
                treeMap.set(key, value)
                loop()
            }
        }
        loop()
        return Log.MemTable(treeMap)
    }

    fun clear(): Unit = raf.setLength(0)

    companion object {
        fun initialize(writeAheadLogFilePath: String): WriteAheadLog {
            val logFile = RandomAccessFile(writeAheadLogFilePath, "rw")

            return WriteAheadLog(logFile)
        }
    }
}