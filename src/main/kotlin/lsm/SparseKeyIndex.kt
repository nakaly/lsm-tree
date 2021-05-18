package lsm

class SparseKeyIndex(val sparseKeys: Array<String>, val keyPositions: Array<Long>) {

    fun positionRange(key: String): Position {
        val right = sparseKeys.binarySearch(key)
        if (right >= 0) {
            return Position.Found(keyPositions[right])
        } else if (right == -1) {
            return Position.NotFound
        } else if (right >= -sparseKeys.size) {
            val rightPos = -(right + 1)
            return Position.Range(keyPositions[rightPos - 1], keyPositions[rightPos])
        } else {
            return Position.Tail(keyPositions[sparseKeys.lastIndex])
        }
    }


    sealed class Position {
        data class Found(val start: Long) : Position()
        data class Range(val start: Long, val end: Long) : Position()
        data class Tail(val start: Long) : Position()
        object NotFound : Position()
    }
}