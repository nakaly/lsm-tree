package lsm

import java.io.RandomAccessFile

class SSTable(val segmentFile: SegmentFile, val sparseKeyIndex: SparseKeyIndex) {

    val sequenceNo: Int = segmentFile.sequenceNo

    fun newReader(): SSTableReader {
        val readOnlyRaf = segmentFile.newReadOnlyRaf()
        return SSTableReader(readOnlyRaf, sparseKeyIndex)
    }

    class SSTableReader(override val readOnlyRaf: RandomAccessFile, val sparseKeyIndex: SparseKeyIndex) :
        SegmentFileReadable {

        private fun rangeSearch(keyBytes: ByteArray, start: Long, end: Long): SegmentFileReadable.Got {
            tailrec fun loop(): SegmentFileReadable.Got {
                if (readOnlyRaf.filePointer < end) {
                    val currentKey = readKeyBytes()
                    return if (currentKey.contentEquals(keyBytes)) {
                        readValue().toGot()
                    } else {
                        skipValue()
                        loop()
                    }
                } else {
                    return SegmentFileReadable.NotFound
                }
            }

            readOnlyRaf.seek(start)
            return loop()
        }

        fun get(key: String): SegmentFileReadable.Got {
            return when (val pos = sparseKeyIndex.positionRange(key)) {
                is SparseKeyIndex.NotFound -> SegmentFileReadable.NotFound
                is SparseKeyIndex.Found -> rangeSearch(key.toByteArray(), pos.start, pos.start + 1)
                is SparseKeyIndex.Range -> rangeSearch(key.toByteArray(), pos.start, pos.end)
                is SparseKeyIndex.Tail -> rangeSearch(key.toByteArray(), pos.start, RAF_LENGTH)
            }
        }


    }


}




