package lsm.sstable

class SparseKeyIndex(val sparseKeys: Array<String>, val keyPositions: Array<Long>) {

    fun positionRange(key: String): Position {
        val right = sparseKeys.binarySearch(key)
        if (right >= 0) {
            return Found(keyPositions[right])
        } else if (right == -1) {
            return NotFound
        } else if (right >= -sparseKeys.size) {
            val rightPos = -(right + 1)
            return Range(keyPositions[rightPos - 1], keyPositions[rightPos])
        } else {
            return Tail(keyPositions[sparseKeys.lastIndex])
        }
    }


    sealed interface Position
    data class Found(val start: Long) : Position
    data class Range(val start: Long, val end: Long) : Position
    data class Tail(val start: Long) : Position
    object NotFound : Position

}