package io.lantern.db

import android.content.SharedPreferences
import java.util.*
import kotlin.collections.HashMap

/**
 * Allows accessing a DB using the SharedPreferences API.
 *
 * @param db the database in which to store the preferences
 * @param prefix a prefix to prepend to all preference keys before storing them in the database
 * @param initialValues the database will be populated with values from this SharedPreferences for any values that haven't already been set (useful for migrating from a regular SharedPreferences)
 */
internal class SharedPreferencesAdapter(
    val db: DB,
    val prefix: String,
    initialValues: SharedPreferences?
) : SharedPreferences {
    private val listenerIds = Collections.synchronizedList(ArrayList<ListenerId>())

    init {
        initialValues?.all?.let {
            db.mutate { tx ->
                it.forEach { (key, value) ->
                    value?.let { tx.putIfAbsent(prefixedPath(key), value) }
                }
            }

        }
    }

    override fun getAll(): MutableMap<String, *> {
        return HashMap(db.list<Any>("${prefix}%").map { unprefixedPath(it.path) to it.value }
            .toMap())
    }

    override fun getString(key: String, defValue: String?): String? {
        return db.get<String>(prefixedPath(key)) ?: defValue
    }

    override fun getStringSet(key: String, defValues: MutableSet<String>?): MutableSet<String> {
        TODO("Not yet implemented")
    }

    override fun getInt(key: String, defValue: Int): Int {
        return db.get<Int>(prefixedPath(key)) ?: defValue
    }

    override fun getLong(key: String, defValue: Long): Long {
        return db.get<Long>(prefixedPath(key)) ?: defValue
    }

    override fun getFloat(key: String, defValue: Float): Float {
        return db.get<Float>(prefixedPath(key)) ?: defValue
    }

    override fun getBoolean(key: String, defValue: Boolean): Boolean {
        return db.get<Boolean>(prefixedPath(key)) ?: defValue
    }

    override fun contains(key: String): Boolean {
        return db.contains(prefixedPath(key))
    }

    override fun edit(): SharedPreferences.Editor {
        return SharedPreferencesEditorAdapter(this)
    }

    override fun registerOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener) {
        val subscriber = object : Subscriber<Any>(UUID.randomUUID().toString(), prefix) {
            override fun onUpdate(path: String, value: Any) {
                listener.onSharedPreferenceChanged(
                    this@SharedPreferencesAdapter,
                    unprefixedPath(path)
                )
            }

            override fun onDelete(path: String) {
                listener.onSharedPreferenceChanged(
                    this@SharedPreferencesAdapter,
                    unprefixedPath(path)
                )
            }
        }
        listenerIds.add(ListenerId(listener, subscriber.id))
        db.subscribe(subscriber)
    }

    override fun unregisterOnSharedPreferenceChangeListener(listener: SharedPreferences.OnSharedPreferenceChangeListener?) {
        val it = listenerIds.iterator()
        while (it.hasNext()) {
            val las = it.next()
            if (las.listener == listener) {
                db.unsubscribe(las.subscriberId)
                it.remove()
                return
            }
        }
    }

    internal fun prefixedPath(path: String): String {
        return prefix + path
    }

    internal fun unprefixedPath(path: String): String {
        return path.substring(prefix.length)
    }
}

internal class SharedPreferencesEditorAdapter(val adapter: SharedPreferencesAdapter) :
    SharedPreferences.Editor {
    private val updates = Collections.synchronizedList(ArrayList<(Transaction) -> Unit>())

    override fun putString(key: String, value: String?): SharedPreferences.Editor {
        if (value == null) {
            remove(key)
        } else {
            updates.add { tx ->
                tx.put(adapter.prefixedPath(key), value)
            }
        }
        return this
    }

    override fun putStringSet(key: String, values: MutableSet<String>?): SharedPreferences.Editor {
        TODO("Not yet implemented")
    }

    override fun putInt(key: String, value: Int): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun putLong(key: String, value: Long): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun putFloat(key: String, value: Float): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun putBoolean(key: String, value: Boolean): SharedPreferences.Editor {
        updates.add { tx ->
            tx.put(adapter.prefixedPath(key), value)
        }
        return this
    }

    override fun remove(key: String): SharedPreferences.Editor {
        updates.add { tx ->
            tx.delete(adapter.prefixedPath(key))
        }
        return this
    }

    override fun clear(): SharedPreferences.Editor {
        updates.add { tx ->
            tx.list<Any>("${adapter.prefix}%").forEach { entry ->
                tx.delete(entry.path)
            }
        }
        return this
    }

    override fun commit(): Boolean {
        apply()
        return true
    }

    override fun apply() {
        adapter.db.mutate { tx ->
            updates.forEach { it(tx) }
        }
        updates.clear()
    }

}

private data class ListenerId(
    val listener: SharedPreferences.OnSharedPreferenceChangeListener,
    val subscriberId: String
)