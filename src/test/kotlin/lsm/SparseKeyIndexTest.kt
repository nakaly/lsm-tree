package lsm

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SparseKeyIndexTest {

    val index = SparseKeyIndex(
        arrayOf("bar", "baz", "foo", "key1", "key2", "key3", "key4"),
        arrayOf(84L, 95L, 194L, 345L, 442L, 809L, 1204L)
    )

    @Test
    fun not_found() {
        assertEquals(index.positionRange("arr"), SparseKeyIndex.Position.NotFound)
    }

    @Test
    fun found() {
        assertEquals(index.positionRange("foo"), SparseKeyIndex.Position.Found(194L))
        assertEquals(index.positionRange("bar"), SparseKeyIndex.Position.Found(84L))
        assertEquals(index.positionRange("key4"), SparseKeyIndex.Position.Found(1204L))
    }

    @Test
    fun range() {
        assertEquals(index.positionRange("car"), SparseKeyIndex.Position.Range(95L, 194L))
        assertEquals(index.positionRange("key1z"), SparseKeyIndex.Position.Range(345L, 442L))
    }

    @Test
    fun tail() {
        assertEquals(index.positionRange("zey"), SparseKeyIndex.Position.Tail(1204L))
        assertEquals(index.positionRange("key4a"), SparseKeyIndex.Position.Tail(1204L))
    }

}