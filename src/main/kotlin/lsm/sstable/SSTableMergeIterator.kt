package lsm.sstable

class SSTableMergeIterator(val sSTables: List<SSTable.SSTableReader>) :
    Iterator<Pair<String, SegmentFileReadable.Value>> {

    private val BUFFER_IS_EMPTY = 0
    private val BUFFER_IS_FILLED = 1
    private val REACHED_EOF = 2

    private val SEGMENT_FILE_SIZE = sSTables.size
    private val emptyBuffer = Array(SEGMENT_FILE_SIZE) { BUFFER_IS_EMPTY }
    private val keyBuffer = Array(SEGMENT_FILE_SIZE) { "" }

    override fun hasNext(): Boolean {
        fulfillBuffer()
        return !emptyBuffer.all { it == REACHED_EOF }
    }

    private fun readNextOf(i: Int) {
        if (emptyBuffer[i] == BUFFER_IS_EMPTY) {
            if (sSTables[i].hasNext()) {
                keyBuffer[i] = sSTables[i].readKey()
                emptyBuffer[i] = BUFFER_IS_FILLED
            } else {
                emptyBuffer[i] = REACHED_EOF
            }
        }
    }

    private fun fulfillBuffer() {
        (0 until SEGMENT_FILE_SIZE).forEach { readNextOf(it) }
    }

    private fun bufferFilledMinKey(): String {
        return (0 until SEGMENT_FILE_SIZE)
            .filter { emptyBuffer[it] == BUFFER_IS_FILLED }
            .map { keyBuffer[it] }
            .minOrNull()!!
    }

    override fun next(): Pair<String, SegmentFileReadable.Value> {
        val minKey = bufferFilledMinKey()

        tailrec fun loopAfterLatest(i: Int, latest: SegmentFileReadable.Value): SegmentFileReadable.Value {
            if (i < SEGMENT_FILE_SIZE) {
                if (emptyBuffer[i] == BUFFER_IS_FILLED && keyBuffer[i] == minKey) {
                    sSTables[i].skipValue()
                    emptyBuffer[i] = BUFFER_IS_EMPTY
                }
                return loopAfterLatest(i + 1, latest)
            } else {
                return latest
            }
        }

        tailrec fun loop(i: Int): SegmentFileReadable.Value {
            if (i < SEGMENT_FILE_SIZE) {
                if (emptyBuffer[i] == BUFFER_IS_FILLED && keyBuffer[i] == minKey) {
                    val latest = sSTables[i].readValue()
                    emptyBuffer[i] = BUFFER_IS_EMPTY
                    return loopAfterLatest(i + 1, latest)
                } else {
                    return loop(i + 1)
                }
            } else {
                return SegmentFileReadable.DeletedValue
            }
        }

        return Pair(minKey, loop(0))
    }

    companion object {
        operator fun invoke(sStables: List<SSTable>): SSTableMergeIterator {
            return SSTableMergeIterator(
                sStables
                    .sortedByDescending { it.sequenceNo }
                    .map { it.newReader() }
            )
        }
    }

}