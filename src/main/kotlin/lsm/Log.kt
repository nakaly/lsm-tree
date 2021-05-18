package lsm

import lsm.SegmentFileReadable.Got
import lsm.SegmentFileReadable.Value
import java.util.*

sealed class Log {
    class MemTable(val index: TreeMap<String, Value>, val maxSize: Int = 10000) : Log() {
        fun isOverMaxSize(): Boolean = index.size >= maxSize
        fun iterator(): MutableIterator<MutableMap.MutableEntry<String, Value>> = index.entries.iterator()

        fun get(key: String): Got {
            return when (val value = index.get(key)) {
                is Value.Exist -> value.toGot()
                is Value.Deleted -> Got.Deleted
                else -> Got.NotFound
            }
        }

        fun set(key: String, value: String) {
            index.set(key, Value.Exist(value))
        }

        fun del(key: String) {
            index.set(key, Value.Deleted)
        }
    }
}