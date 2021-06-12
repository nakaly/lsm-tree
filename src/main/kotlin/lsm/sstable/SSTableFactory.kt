package lsm.sstable

import java.io.RandomAccessFile

class SSTableFactory(private val sparseIndexPer: Int, private val segmentFileBathPath: String) {

    private fun writeKeyValue(
        key: String,
        value: SegmentFileReadable.Value,
        raf: RandomAccessFile
    ) {
        raf.writeInt(key.length)
        raf.writeBytes(key)
        when (value) {
            is SegmentFileReadable.Exist -> {
                raf.writeInt(value.value.length)
                raf.writeBytes(value.value)
            }
            is SegmentFileReadable.DeletedValue -> raf.writeInt(SegmentFile.TOMBSTONE)
        }
    }

    fun apply(
        sequenceNo: Int,
        iterator: Iterator<Pair<String, SegmentFileReadable.Value>>
    ): SSTable {
        val segmentFile = SegmentFile(sequenceNo, segmentFileBathPath)
        segmentFile.createSegmentFile()
        val raf = segmentFile.newReadWriteRaf()

        val keyIndex = mutableListOf<String>()
        val positionIndex = mutableListOf<Long>()

        tailrec fun loop(idx: Int) {
            if (iterator.hasNext()) {
                val currentPosition = raf.filePointer
                val (key, value) = iterator.next()
                writeKeyValue(key, value, raf)

                if (idx % sparseIndexPer == 0) {
                    keyIndex.add(key)
                    positionIndex.add(currentPosition)
                }
                loop(idx + 1)
            }
        }

        try {
            loop(0)
        } finally {
            raf.close()
        }
        val sparseKeyIndex = SparseKeyIndex(keyIndex.toTypedArray(), positionIndex.toTypedArray())
        return SSTable(segmentFile, sparseKeyIndex)
    }

    fun recovery(sequenceNo: Int): SSTable {
        val segmentFile = SegmentFile(sequenceNo, segmentFileBathPath)
        val reader = SSTableReader(segmentFile.newReadWriteRaf())
        try {
            val keyIndex = mutableListOf<String>()
            val positionIndex = mutableListOf<Long>()

            fun pickupKeys() {
                tailrec fun recursive(idx: Int) {
                    if (reader.hasNext()) {
                        if (idx % sparseIndexPer == 0) {
                            val pointer = reader.currentPointer()
                            val key = reader.readKey()
                            keyIndex.add(key)
                            positionIndex.add(pointer)

                        } else {
                            reader.skipKey()
                        }
                        reader.skipValue()
                        recursive(idx + 1)
                    }
                }
                recursive(0)
            }
            pickupKeys()
            val sparseKeyIndex = SparseKeyIndex(keyIndex.toTypedArray(), positionIndex.toTypedArray())
            return SSTable(segmentFile, sparseKeyIndex)
        } finally {
            reader.close()
        }
    }

    companion object {
        private class SSTableReader(override val readOnlyRaf: RandomAccessFile) : SegmentFileReadable
    }

}