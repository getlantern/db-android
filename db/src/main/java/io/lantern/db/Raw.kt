package io.lantern.db

/**
 * The raw bytes with the ability to obtain the deserialized value
 */
class Raw<T : Any> internal constructor(
    private val serde: Serde,
    private val allBytes: ByteArray,
    get: Lazy<T>
) {
    val bytes: ByteArray by lazy { serde.rawWithoutHeader(allBytes) }
    val value: T by get
    val valueOrProtoBytes: Any by lazy { serde.deserializedOrProtoBytes(allBytes) }

    internal constructor(serde: Serde, bytes: ByteArray) : this(
        serde,
        bytes,
        lazy { serde.deserialize(bytes) })

    internal constructor(serde: Serde, bytes: ByteArray, value: T) : this(
        serde,
        bytes,
        lazyOf(value)
    )

    internal constructor(serde: Serde, value: T) : this(serde, serde.serialize(value), value)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Raw<*>

        if (!allBytes.contentEquals(other.allBytes)) return false

        return true
    }

    override fun hashCode(): Int {
        return allBytes.contentHashCode()
    }
}