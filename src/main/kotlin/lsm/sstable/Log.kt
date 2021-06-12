package lsm.sstable

import lsm.sstable.SegmentFileReadable.Got
import lsm.sstable.SegmentFileReadable.Value
import java.util.*

sealed class Log {
    class MemTable(val index: TreeMap<String, Value>, val maxSize: Int = 3) : Log() {
        fun isOverMaxSize(): Boolean = index.size >= maxSize
        fun iterator(): Iterator<Pair<String, Value>> = index.entries.map { Pair(it.key, it.value) }.iterator()

        fun get(key: String): Got {
            return when (val value = index.get(key)) {
                is SegmentFileReadable.Exist -> value.toGot()
                is SegmentFileReadable.DeletedValue -> SegmentFileReadable.Deleted
                else -> SegmentFileReadable.NotFound
            }
        }

        fun set(key: String, value: String) {
            index.set(key, SegmentFileReadable.Exist(value))
        }

        fun del(key: String) {
            index.set(key, SegmentFileReadable.DeletedValue)
        }
    }

    class SSTableRef(val sStable: SSTable) : Log()
}