package io.lantern.db

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Registration
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.google.gson.Gson
import com.google.protobuf.GeneratedMessageLite
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap

/**
 * Serde provides a serialization/deserialization mechanism that stores values as follows:
 *
 * String -> T<string>
 * ByteArray -> A<bytes>
 * Byte -> B<byte>
 * Short -> S<short>
 * Int -> I<int>
 * Long -> L<long>
 * Float -> F<float>
 * Double -> D<double>
 * Char -> C<char>
 * Protocol Buffer -> P<protocol buffer serialized>
 * Everything else -> J<JSON serialized>
 *
 * Anything that is to be serialized as a JSON object needs to be registered by calling register
 * first.
 *
 * For backward compatibility purposes, values that used to be stored as Kryo serialized can still
 * be read, but on subsequent writes they'll be written with the appropriate primitive data type or
 * with JSON.
 */

internal class Serde {
    private val kryo = Kryo()
    private val gson = Gson()
    private val registeredProtocolBufferTypes =
        ConcurrentHashMap<Class<GeneratedMessageLite<*, *>>, Int>()
    private val registeredProtocolBufferParsers =
        ConcurrentHashMap<Short, (InputStream) -> GeneratedMessageLite<*, *>>()
    private val registeredJsonTypes =
        ConcurrentHashMap<Class<*>, Int>()
    private val registeredJsonTypeIds =
        ConcurrentHashMap<Short, Class<*>>()

    init {
        kryo.isRegistrationRequired = false
    }

    @Synchronized
    internal fun <T> register(id: Short, type: Class<T>) {
        if (id < 20) {
            // Kryo uses ids between 0-9 for primitive types, don't interfere with those. To be safe, leave extra room.
            throw IllegalArgumentException("attempted to register ID below 20")
        }
        if (GeneratedMessageLite::class.java.isAssignableFrom(type)) {
            val pbufType = type as Class<GeneratedMessageLite<*, *>>
            val parseMethod = pbufType.getMethod("parseFrom", InputStream::class.java)
            registeredProtocolBufferTypes[pbufType] = id.toInt()
            registeredProtocolBufferParsers[id] =
                { stream -> parseMethod.invoke(pbufType, stream) as GeneratedMessageLite<*, *> }
        } else {
            kryo.register(Registration(type, kryo.getDefaultSerializer(type), id.toInt()))
            registeredJsonTypes[type] = id.toInt()
            registeredJsonTypeIds[id] = type
        }
    }

    internal fun serialize(data: Any): ByteArray {
        val out = ByteArrayOutputStream()
        val dataOut = DataOutputStream(out)

        when (data) {
            is String -> {
                // Write strings in optimized format that preserves sort order
                dataOut.write(TEXT)
                dataOut.write(data.toByteArray(charset))
            }
            is ByteArray -> {
                dataOut.write(BYTEARRAY)
                dataOut.write(data)
            }
            is Byte -> {
                dataOut.write(BYTE)
                dataOut.writeByte(data.toInt())
            }
            is Boolean -> {
                dataOut.write(BOOLEAN)
                dataOut.writeBoolean(data)
            }
            is Short -> {
                dataOut.write(SHORT)
                dataOut.writeShort(data.toInt())
            }
            is Int -> {
                dataOut.write(INT)
                dataOut.writeInt(data)
            }
            is Long -> {
                dataOut.write(LONG)
                dataOut.writeLong(data)
            }
            is Float -> {
                dataOut.write(FLOAT)
                dataOut.writeFloat(data)
            }
            is Double -> {
                dataOut.write(DOUBLE)
                dataOut.writeDouble(data)
            }
            is Char -> {
                dataOut.write(CHAR)
                dataOut.writeChar(data.code)
            }
            is GeneratedMessageLite<*, *> -> {
                val pbufTypeId = registeredProtocolBufferTypes[data::class.java]
                if (pbufTypeId != null) {
                    // Serialize using protocol buffers
                    dataOut.write(PROTOCOL_BUFFER)
                    dataOut.writeShort(pbufTypeId)
                    data.writeTo(dataOut)
                } else {
                    throw AssertionError("Attempted to serialize unregistered protocol buffer type ${data::class.java}, please first register this type by calling register()")
                }
            }
            else -> {
                val jsonTypeId = registeredJsonTypes[data::class.java]
                if (jsonTypeId != null) {
                    // Serialize using JSON
                    dataOut.write(JSON)
                    dataOut.writeShort(jsonTypeId)
                    dataOut.writeUTF(gson.toJson(data))
                } else {
                    throw AssertionError("Attempted to serialize unregistered type ${data::class.java}, please first register this type by calling register()")
                }
            }
        }

        dataOut.close()
        return out.toByteArray()
    }

    // old-school form of serialization using kryo, only used for testing
    internal fun serializeLegacyMode(data: Any): ByteArray {
        val out = ByteArrayOutputStream()
        val dataOut = DataOutputStream(out)

        when (data) {
            is String -> {
                // Write strings in optimized format that preserves sort order
                dataOut.write(TEXT)
                dataOut.write(data.toByteArray(charset))
            }
            is GeneratedMessageLite<*, *> -> {
                val pbufTypeId = registeredProtocolBufferTypes[data::class.java]
                if (pbufTypeId != null) {
                    // Serialize using protocol buffers
                    dataOut.write(PROTOCOL_BUFFER)
                    dataOut.writeShort(pbufTypeId)
                    data.writeTo(dataOut)
                } else {
                    throw AssertionError("Attempted to serialize unregistered protocol buffer type ${data::class.java}, please first register this type by calling register()")
                }
            }
            else -> {
                // Write everything else with Kryo
                dataOut.write(KRYO)
                val kryoOut = Output(dataOut)
                kryo.writeClassAndObject(kryoOut, data)
                kryoOut.close()
            }
        }

        dataOut.close()
        return out.toByteArray()
    }

    internal fun <D> deserialize(bytes: ByteArray): D {
        val dataIn = DataInputStream(ByteArrayInputStream(bytes))
        return deserialize(dataIn, dataIn.read())
    }

    private fun <D> deserialize(dataIn: DataInputStream, type: Int): D {
        return when (type) {
            TEXT -> dataIn.readBytes().toString(charset) as D
            BYTEARRAY -> dataIn.readBytes() as D
            BYTE -> dataIn.readByte() as D
            BOOLEAN -> dataIn.readBoolean() as D
            SHORT -> dataIn.readShort() as D
            INT -> dataIn.readInt() as D
            LONG -> dataIn.readLong() as D
            FLOAT -> dataIn.readFloat() as D
            DOUBLE -> dataIn.readDouble() as D
            CHAR -> dataIn.readChar() as D
            PROTOCOL_BUFFER -> {
                val pbufTypeId = dataIn.readShort()


                val pbufParser = registeredProtocolBufferParsers[pbufTypeId]
                    ?: throw RuntimeException("Attempt to deserialize unregistered protocol buffer type id $pbufTypeId")

                
                pbufParser(dataIn) as D
            }
            JSON -> {
                val jsonTypeId = dataIn.readShort()
                val type = registeredJsonTypeIds[jsonTypeId]
                    ?: throw RuntimeException("Attempt to deserialize unregistered JSON type id $jsonTypeId")
                gson.fromJson(dataIn.readUTF(), type) as D
            }
            else -> kryo.readClassAndObject(Input(dataIn)) as D
        }
    }

    internal fun rawWithoutHeader(bytes: ByteArray): ByteArray {
        val dataIn = DataInputStream(ByteArrayInputStream(bytes))
        return when (dataIn.read()) {
            TEXT -> dataIn.readBytes()
            PROTOCOL_BUFFER -> {
                // consume the length
                dataIn.readShort()
                // return remaining bytes
                dataIn.readBytes()
            }
            else -> throw AssertionError("rawWithoutHeader not supported for JSON or Kryo serialized values")
        }
    }

    /**
     * For protocol buffer objects, this returns the protocol buffer bytes, otherwise returns the
     * deserialized value.
     */
    internal fun deserializedOrProtoBytes(bytes: ByteArray): Any {
        val dataIn = DataInputStream(ByteArrayInputStream(bytes))
        return when (val type = dataIn.read()) {
            TEXT -> dataIn.readBytes().toString(charset)
            PROTOCOL_BUFFER -> {
                // consume the length
                dataIn.readShort()
                // return remaining bytes
                dataIn.readBytes()
            }
            else -> deserialize(dataIn, type)
        }
    }

    companion object {
        private const val TEXT = 'T'.code
        private const val KRYO = 'K'.code
        private const val PROTOCOL_BUFFER = 'P'.code
        private const val JSON = 'J'.code
        private const val BYTEARRAY = 'A'.code
        private const val BYTE = '2'.code
        private const val BOOLEAN = 'B'.code
        private const val SHORT = 'S'.code
        private const val INT = 'I'.code
        private const val LONG = 'L'.code
        private const val FLOAT = 'F'.code
        private const val DOUBLE = 'D'.code
        private const val CHAR = 'C'.code
        private val charset = Charset.defaultCharset()
    }
}
