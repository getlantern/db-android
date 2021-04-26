package io.lantern.db

import android.content.Context
import android.content.SharedPreferences
import ca.gedge.radixtree.RadixTree
import ca.gedge.radixtree.RadixTreeVisitor
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import net.sqlcipher.Cursor
import net.sqlcipher.database.SQLiteConstraintException
import net.sqlcipher.database.SQLiteDatabase
import java.io.Closeable
import java.io.File
import java.util.HashSet
import java.util.TreeSet
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

data class PathAndValue<T>(val path: String, val value: T)

data class Detail<T>(val path: String, val detailPath: String, val value: T)

data class ChangeSet<T : Any>(
    val updates: Map<String, T> = emptyMap(),
    val deletions: Set<String> = emptySet()
)

data class RawChangeSet<T : Any>(
    val updates: Map<String, Raw<T>> = emptyMap(),
    val deletions: Set<String> = emptySet(),
)

data class DetailsChangeSet<T : Any>(
    /**
     * Any updated paths, keyed to the subscribed path.
     */
    val updates: Map<String, PathAndValue<Raw<T>>> = emptyMap(),

    /**
     * Any deleted paths.
     */
    val deletions: Set<String> = emptySet(),
)

/**
 * Subscriber for path update events.
 *
 * id - unique identifier for this subscriber
 * pathPrefixes - subscriber will receive notifications for all changes under these path prefixes
 */
abstract class RawSubscriber<T : Any>(
    internal val id: String,
    vararg pathPrefixes: String
) {
    // clean path prefixes in case they included an unnecessary trailing %
    internal val cleanedPathPrefixes = pathPrefixes.map { it.trimEnd('%') }

    internal open fun onInitial(values: List<PathAndValue<Raw<T>>>) {
        onChanges(RawChangeSet(updates = values.map { it.path to it.value }.toMap()))
    }

    /**
     * Called when some values change
     */
    abstract fun onChanges(changes: RawChangeSet<T>)
}

/**
 * Like RawSubscriber but receiving the value directly rather than wrapped in a Raw.
 */
abstract class Subscriber<T : Any>(id: String, vararg pathPrefixes: String) :
    RawSubscriber<T>(id, *pathPrefixes) {

    override fun onChanges(changes: RawChangeSet<T>) {
        onChanges(
            ChangeSet(
                updates = changes.updates.map { it.key to it.value.value }.toMap(),
                deletions = changes.deletions
            )
        )
    }

    abstract fun onChanges(changes: ChangeSet<T>)
}

/**
 * A subscriber for updates to details for the paths matching its pathPrefix.
 *
 * The values corresponding to paths matching the pathPrefix are themselves treated as paths
 * with which to look up the details.
 *
 * For example, given the following data:
 *
 * {"/detail/1": "one",
 *  "/detail/2": "two",
 *  "/list/1": "/detail/2",
 *  "/list/2": "/detail/1"}
 *
 * A details subscription to prefix "/list/" would include ["one", "two"]. It would be notified
 * if the paths /detail/1 or /detail/2 change, or if a new item is added to /list/ or an item
 * is deleted from /list/.
 */
abstract class DetailsSubscriber<T : Any>(
    id: String,
    vararg pathPrefixes: String
) : RawSubscriber<Any>(
    id,
    "%"
) {
    internal lateinit var db: DB
    private val subscribedPaths = RadixTree<Boolean>()
    internal val subscribedDetailPathsToOriginalPaths = HashMap<String, String>()
    internal val subscribedOriginalPathsToDetailPaths = HashMap<String, String>()

    init {
        pathPrefixes.map {
            it.trimEnd('%')
        }.forEach { path -> subscribedPaths[path] = true }
    }

    abstract fun onChanges(changes: DetailsChangeSet<T>)

    override fun onChanges(changes: RawChangeSet<Any>) {
        // process deletions first
        val deletions = HashSet<String>()
        changes.deletions.forEach { path ->
            subscribedDetailPathsToOriginalPaths.remove(path)
                ?.let { originalPath -> deletions.add(originalPath) }
            subscribedPaths.visit(object :
                    RadixTreeVisitor<Boolean, Void?> {
                    override fun visit(key: String?, value: Boolean?): Boolean {
                        if (key == null || !path.startsWith(key)) {
                            return false
                        }
                        deletions.add(path)
                        subscribedOriginalPathsToDetailPaths.remove(path)
                            ?.let { subscribedDetailPathsToOriginalPaths.remove(it) }
                        return true
                    }

                    override fun getResult(): Void? {
                        return null
                    }
                })
        }

        // then update our detail subscription paths
        val updatedPaths = HashMap<String, String>()
        changes.updates.forEach { (path, value) ->
            subscribedPaths.visit(object :
                    RadixTreeVisitor<Boolean, Void?> {
                    override fun visit(key: String?, v: Boolean?): Boolean {
                        if (key == null || !path.startsWith(key)) {
                            return false
                        }
                        val detailPath = value.value as String
                        subscribedOriginalPathsToDetailPaths[path] = detailPath
                        subscribedDetailPathsToOriginalPaths[detailPath] = path
                        updatedPaths[path] = detailPath
                        return true
                    }

                    override fun getResult(): Void? {
                        return null
                    }
                })
        }

        // then capture updates to details
        val updates = HashMap<String, PathAndValue<Raw<T>>>()
        changes.updates.forEach { (detailPath, value) ->
            subscribedDetailPathsToOriginalPaths[detailPath]
                ?.let { originalPath ->
                    updates[originalPath] = PathAndValue(detailPath, value as Raw<T>)
                    updatedPaths.remove(originalPath)
                }
        }

        // lastly capture values for any updated paths that didn't get an update from the corresponding detail path
        updatedPaths.forEach { (path, detailPath) ->
            db.getRaw<T>(detailPath)?.let { updates[path] = PathAndValue(detailPath, it) }
        }

        // now notify original subscriber
        onChanges(DetailsChangeSet(updates = updates, deletions = deletions))
    }
}

/**
 * Indicates that a call to findOne found more than one matches.
 */
class TooManyMatchesException : Exception("More than one value matched path query")

/**
 * Provides a simple key/value store with a map-like interface. It allows the registration of
 * subscribers that observe changes to values at key paths.
 */
class DB private constructor(
    db: SQLiteDatabase,
    schema: String,
    private val currentTransaction: ThreadLocal<Transaction>,
    private val savepointSequence: AtomicInteger,
    private val txExecutor: ExecutorService,
    private val publishExecutor: ExecutorService,
    private val derived: Boolean = false
) :
    Queryable(db, schema, Serde()), Closeable {
    private val subscribers = RadixTree<PersistentMap<String, RawSubscriber<Any>>>()
    private val subscribersById = ConcurrentHashMap<String, RawSubscriber<Any>>()

    private constructor(db: SQLiteDatabase, schema: String, name: String) : this(
        db,
        schema,
        ThreadLocal(),
        AtomicInteger(),
        Executors.newSingleThreadExecutor {
            Thread(it, "$name-tx-executor")
        },
        Executors.newSingleThreadExecutor {
            Thread(it, "$name-publish-executor")
        }
    )

    init {
        if (schema.contains(Regex("\\s"))) {
            throw IllegalArgumentException("database name must not contain whitespace")
        }

        // All data is stored in a single table that has a TEXT path, a BLOB value. The table is
        // stored as an index organized table (WITHOUT ROWID option) as a performance
        // optimization for range scans on the path. To support full text indexing with an
        // external content fts5 table, we include a manually managed INTEGER rowid to which we
        // can join the fts5 virtual table. Rows that are not full text indexed have a null
        // to save space.
        db.execSQL(
            """CREATE TABLE IF NOT EXISTS ${schema}_data
                        (path TEXT PRIMARY KEY, value BLOB, rowid INTEGER) WITHOUT ROWID"""
        )
        // Create an index on only text values to speed up detail lookups that join on path = value
        db.execSQL(
            """CREATE INDEX IF NOT EXISTS ${schema}_data_value_index ON ${schema}_data(value)
                        WHERE SUBSTR(CAST(value AS TEXT), 1, 1) = 'T'"""
        )
        // Create a table for full text search
        db.execSQL(
            """CREATE VIRTUAL TABLE IF NOT EXISTS ${schema}_fts
                        USING fts5(value, content=data, tokenize='porter unicode61')"""
        )
        // Create a table for managing custom counters (currently used only for full text indexing)
        db.execSQL(
            """CREATE TABLE IF NOT EXISTS ${schema}_counters
                   (id INTEGER PRIMARY KEY, value INTEGER)""".trimMargin()
        )
    }

    /**
     * Returns a DB backed by the same underlying SQLLite database, but saving data in its own set
     * of tables (a "schema").
     */
    fun withSchema(schema: String) =
        DB(
            db,
            schema,
            currentTransaction,
            savepointSequence,
            txExecutor,
            publishExecutor,
            derived = true
        )

    companion object {
        /**
         * Builds a DB backed by an encrypted SQLite database at the given filePath
         *
         * collectionName - the name of the map as stored in the database
         * password - the password used to encrypted the data (the longer the better)
         */
        fun createOrOpen(
            ctx: Context,
            filePath: String,
            password: String,
            secureDelete: Boolean = true,
            schema: String = "default",
            name: String = File(filePath).name
        ): DB {
            // TODO: if the process crashes in the middle of creating the DB, the next time we start
            // up we can get SQLiteException: file is not a database android. We should try to clean
            // this up automatically, but be careful to not delete legitimate data in case it was
            // corrupted in a different way.
            SQLiteDatabase.loadLibs(ctx)
            val db = SQLiteDatabase.openOrCreateDatabase(filePath, password, null)
            if (!db.enableWriteAheadLogging()) {
                throw RuntimeException("Unable to enable write ahead logging")
            }
            if (secureDelete) {
                // Enable secure delete
                db.query("PRAGMA secure_delete;").use { cursor ->
                    if (cursor == null || !cursor.moveToNext()) {
                        throw RuntimeException("Unable to enable secure delete")
                    }
                }
            }
            return DB(db, schema, name)
        }
    }

    /**
     * Registers a type for optimized serialization. Protocol Buffer types will be serialized with
     * protocol buffers, all others with Kryo.
     *
     * The id identifies the type in serialized values.
     *
     * The id MUST be an short integer 10 or above
     * The id MUST be consistent over time - registering the same class under different IDs will
     * cause incompatibilities with previously stored data.
     */
    fun <T> registerType(id: Short, type: Class<T>) {
        serde.register(id, type)
    }

    /**
     * Registers a subscriber for any updates to the paths matching its pathPrefix.
     *
     * If receiveInitial is true, the subscriber will immediately be called for all matching values.
     */
    fun <T : Any> subscribe(
        subscriber: RawSubscriber<T>,
        receiveInitial: Boolean = true
    ) {
        txExecute {
            doSubscribe(subscriber, receiveInitial)
        }
    }

    private fun <T : Any> doSubscribe(
        subscriber: RawSubscriber<T>,
        receiveInitial: Boolean = true
    ) {
        if (subscribersById.putIfAbsent(
                subscriber.id,
                subscriber as RawSubscriber<Any>
            ) != null
        ) {
            throw IllegalArgumentException("subscriber with id ${subscriber.id} already registered")
        }

        subscriber.cleanedPathPrefixes.forEach { pathPrefix ->
            val subscribersForPrefix = subscribers[pathPrefix]?.put(
                subscriber.id,
                subscriber
            ) ?: persistentHashMapOf(subscriber.id to subscriber)
            subscribers[pathPrefix] = subscribersForPrefix

            if (receiveInitial) {
                subscriber.onInitial(listRaw<T>("$pathPrefix%"))
            }
        }
    }

    fun <T : Any> subscribe(
        subscriber: DetailsSubscriber<T>,
        receiveInitial: Boolean = true
    ) {
        subscriber.db = this
        txExecute {
            doSubscribeDetails(subscriber, receiveInitial)
        }
    }

    private fun <T : Any> doSubscribeDetails(
        subscriber: DetailsSubscriber<T>,
        receiveInitial: Boolean = true
    ) {
        doSubscribe(subscriber, receiveInitial = false)
        if (receiveInitial) {
            subscriber.cleanedPathPrefixes.forEach { pathPrefix ->
                val list = listDetailsRaw<T>("$pathPrefix%")
                val updates = HashMap<String, Raw<Any>>()
                list.forEach { detail ->
                    // this is the mapping of original path to detail path
                    updates[detail.path] = Raw(serde, detail.detailPath)
                    // this is the actual detail
                    updates[detail.detailPath] = detail.value as Raw<Any>
                }
                subscriber.onChanges(RawChangeSet(updates = updates))
            }
        }
    }

    /**
     * Unsubscribes the subscriber identified by subscriberId
     */
    fun unsubscribe(subscriberId: String) {
        txExecutor.submit(
            Callable<Unit> {
                val subscriber = subscribersById.remove(subscriberId)
                subscriber?.cleanedPathPrefixes?.forEach { pathPrefix ->
                    val subscribersForPrefix =
                        subscribers[pathPrefix]?.remove(subscriber.id)
                    if (subscribersForPrefix?.size ?: 0 == 0) {
                        subscribers.remove(pathPrefix)
                    } else {
                        subscribers[pathPrefix] = subscribersForPrefix
                    }
                }
            }
        )
    }

    /**
     * Mutates the database inside of a transaction.
     *
     * If the callback function throws an exception, the entire transaction is rolled back.
     *
     * If the callback completes without exception, the entire transaction is committed and all
     * listeners of affected key paths are notified.
     *
     * All mutating happens on a single thread. Nested calls to mutate are allowed and will
     * each get their own sub-transaction implemented using savepoints.
     */
    fun <T> mutate(publishBlocking: Boolean = false, fn: (tx: Transaction) -> T): T {
        var inExecutor = false
        val tx = synchronized(this) {
            val currentTx = currentTransaction.get()
            if (currentTx == null) {
                Transaction(db, schema, serde, HashMap(mapOf(schema to subscribers)))
            } else {
                inExecutor = true
                currentTx
            }
        }

        return if (inExecutor) {
            // we're already in the executor thread, do the work with a savepoint
            tx.subscribersBySchema[schema] = subscribers
            val nestedTx = Transaction(
                db,
                schema,
                serde,
                tx.subscribersBySchema,
                tx.updatesBySchema,
                tx.deletionsBySchema,
                "save_${savepointSequence.incrementAndGet()}"
            )
            try {
                nestedTx.beginSavepoint()
                currentTransaction.set(nestedTx)
                val result = fn(nestedTx)
                nestedTx.setSavepointSuccessful()
                result
            } finally {
                nestedTx.endSavepoint()
                currentTransaction.set(tx)
            }
        } else {
            // schedule the work to run in our single threaded executor
            val future = txExecutor.submit(
                Callable {
                    try {
                        db.beginTransaction()
                        currentTransaction.set(tx)
                        val result = fn(tx)
                        db.setTransactionSuccessful()
                        result
                    } finally {
                        db.endTransaction()
                        currentTransaction.remove()
                    }
                }
            )
            try {
                val result = future.get()
                // publish outside of the txExecutor
                val publishResult = publishExecutor.submit { tx.publish() }
                if (publishBlocking) {
                    publishResult.get()
                }
                return result
            } catch (e: ExecutionException) {
                throw e.cause ?: e
            }
        }
    }

    fun <T> mutatePublishBlocking(fn: (tx: Transaction) -> T): T = mutate(true, fn)

    /**
     * Returns a SharedPreferences backed by this db.
     *
     * @param schema - preference keys are stored in this named schema
     * @param fallback - an optional fallback SharedPreferences to use for values that aren't found in the db
     */
    fun asSharedPreferences(
        schema: String,
        fallback: SharedPreferences? = null
    ): SharedPreferences {
        return SharedPreferencesAdapter(this.withSchema(schema), fallback)
    }

    private fun <T> txExecute(cmd: Callable<T>): T {
        try {
            return txExecutor.submit(cmd).get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
    }

    @Synchronized
    override fun close() {
        if (derived) {
            // ignore close on derived databases
            return
        }
        txExecutor.shutdownNow()
        publishExecutor.shutdownNow()
        txExecutor.awaitTermination(10, TimeUnit.SECONDS)
        publishExecutor.awaitTermination(10, TimeUnit.SECONDS)
        db.close()
    }
}

open class Queryable internal constructor(
    protected val db: SQLiteDatabase,
    internal val schema: String,
    internal val serde: Serde
) {
    /**
     * Gets the value at the given path
     */
    fun <T : Any> get(path: String): T? {
        selectSingle(path).use { cursor ->
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            return serde.deserialize(cursor.getBlob(0))
        }
    }

    /**
     * Gets the raw value at the given path
     */
    fun <T : Any> getRaw(path: String): Raw<T>? {
        selectSingle(path).use { cursor ->
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            return Raw(serde, cursor.getBlob(0))
        }
    }

    /**
     * Indicates whether the db contains a value at the given path
     */
    fun contains(path: String): Boolean {
        db.rawQuery(
            "SELECT COUNT(path) FROM ${schema}_data WHERE path = ?",
            arrayOf(serde.serialize(path))
        ).use { cursor ->
            return cursor != null && cursor.moveToNext() && cursor.getInt(0) > 0
        }
    }

    private fun selectSingle(path: String): Cursor {
        return db.rawQuery(
            "SELECT value FROM ${schema}_data WHERE path = ?",
            arrayOf(serde.serialize(path))
        )
    }

    /**
     * Retrieves the details path stored at path and then retrieves the value that details path
     * points to. Same logic as #listDetails.
     */
    fun <T : Any> getDetail(path: String): T? {
        db.rawQuery(
            "SELECT d.value FROM ${schema}_data l INNER JOIN ${schema}_data d ON l.value = d.path WHERE l.path = ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T'",
            arrayOf(serde.serialize(path))
        ).use { cursor ->
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            val serialized = cursor.getBlob(0)
            if (cursor.moveToNext()) {
                throw TooManyMatchesException()
            }
            return serde.deserialize(serialized)
        }
    }

    /**
     * Gets the single value matching a path query. If there are no values matching the path query,
     * this returns null.
     *
     * @throws TooManyMatchesException If there are more than one values matching the query
     */
    @Throws(TooManyMatchesException::class)
    fun <T : Any> findOne(pathQuery: String): T? {
        return findOneRaw<T>(pathQuery)?.value?.value
    }

    /**
     * Like findOne, but returning the raw value and path
     */
    @Throws(TooManyMatchesException::class)
    fun <T : Any> findOneRaw(pathQuery: String): PathAndValue<Raw<T>>? {
        db.rawQuery(
            "SELECT path, value FROM ${schema}_data WHERE path LIKE(?)",
            arrayOf(serde.serialize(pathQuery))
        ).use { cursor ->
            if (cursor == null || !cursor.moveToNext()) {
                return null
            }
            val path = cursor.getBlob(0)
            val value = cursor.getBlob(1)
            if (cursor.moveToNext()) {
                throw TooManyMatchesException()
            }
            return PathAndValue(serde.deserialize(path), Raw(serde, value))
        }
    }

    /**
     * Lists all values matching the pathQuery. A path query is a path with '%' used as a wildcard.
     *
     * For example, given the following data:
     *
     * {"/detail/1": "one",
     *  "/detail/2": "two"}
     *
     * The pathQuery "/detail/%" would return ["one", "two"]
     *
     * By default results are sorted lexicographically by path. If reverseSort is specified, that is
     * reversed.
     *
     * If fullTextSearch is specified, in addition to the pathQuery, rows will be filtered by
     * searching for matches to the fullTextSearch term in the full text index.
     */
    fun <T : Any> list(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<PathAndValue<T>> {
        val result = ArrayList<PathAndValue<T>>()
        doList(pathQuery, start, count, fullTextSearch, reverseSort) { cursor ->
            result.add(
                PathAndValue(
                    serde.deserialize(cursor.getBlob(0)),
                    serde.deserialize(cursor.getBlob(1))
                )
            )
        }
        return result
    }

    /**
     * Like list but returning the raw values
     */
    fun <T : Any> listRaw(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<PathAndValue<Raw<T>>> {
        val result = ArrayList<PathAndValue<Raw<T>>>()
        doList(pathQuery, start, count, fullTextSearch, reverseSort) { cursor ->
            result.add(
                PathAndValue(
                    serde.deserialize(cursor.getBlob(0)),
                    Raw(serde, cursor.getBlob(1))
                )
            )
        }
        return result
    }

    /**
     * Like list but only lists the paths of matching rows.
     */
    fun listPaths(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<String> {
        val result = ArrayList<String>()
        doList(pathQuery, start, count, fullTextSearch, reverseSort) { cursor ->
            result.add(serde.deserialize(cursor.getBlob(0)))
        }
        return result
    }

    private fun doList(
        pathQuery: String,
        start: Int,
        count: Int,
        fullTextSearch: String?,
        reverseSort: Boolean,
        onRow: (cursor: Cursor) -> Unit
    ) {
        val cursor = if (fullTextSearch != null) {
            db.rawQuery(
                "SELECT d.path, d.value FROM ${schema}_fts f INNER JOIN ${schema}_data d ON f.rowid = d.rowid WHERE d.path LIKE ? AND f.value MATCH ? ORDER BY f.rank LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), fullTextSearch, count, start)
            )
        } else {
            val sortOrder = if (reverseSort) "DESC" else "ASC"
            db.rawQuery(
                "SELECT path, value FROM ${schema}_data WHERE path LIKE ? ORDER BY path $sortOrder LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), count, start)
            )
        }
        cursor.use {
            if (cursor != null) {
                while (cursor.moveToNext()) {
                    onRow(cursor)
                }
            }
        }
    }

    /**
     * Lists details for paths matching the pathQuery, where details are found by treating the
     * values of the matching paths as paths to look up the details.
     *
     * For example, given the following data:
     *
     * {"/detail/1": "one",
     *  "/detail/2": "two",
     *  "/list/1": "/detail/2",
     *  "/list/2": "/detail/1"}
     *
     * The pathQuery "/list/%" would return ["two", "one"]
     *
     * If fullTextSearch is specified, in addition to the pathQuery, detail rows will be filtered by
     * searching for matches to the fullTextSearch term in the full text index corresponding to the
     * detail rows (not the top level list).
     */
    fun <T : Any> listDetails(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<Detail<T>> {
        val result = ArrayList<Detail<T>>()
        doListDetails<T>(
            pathQuery,
            start,
            count,
            fullTextSearch,
            reverseSort
        ) { listPath, detailPath, value ->
            result.add(Detail(listPath, detailPath, serde.deserialize(value)))
        }
        return result
    }

    /**
     * Like listDetails but returning the raw values
     */
    fun <T : Any> listDetailsRaw(
        pathQuery: String,
        start: Int = 0,
        count: Int = Int.MAX_VALUE,
        fullTextSearch: String? = null,
        reverseSort: Boolean = false
    ): List<Detail<Raw<T>>> {
        val result = ArrayList<Detail<Raw<T>>>()
        doListDetails<T>(
            pathQuery,
            start,
            count,
            fullTextSearch,
            reverseSort
        ) { listPath, detailPath, value ->
            result.add(Detail(listPath, detailPath, Raw(serde, value)))
        }
        return result
    }

    private fun <T : Any> doListDetails(
        pathQuery: String,
        start: Int,
        count: Int,
        fullTextSearch: String?,
        reverseSort: Boolean,
        onResult: (listPath: String, detailPath: String, value: ByteArray) -> Unit
    ) {
        val cursor = if (fullTextSearch != null) {
            db.rawQuery(
                "SELECT l.path, d.path, d.value FROM ${schema}_data l INNER JOIN ${schema}_data d ON l.value = d.path INNER JOIN ${schema}_fts f ON f.rowid = d.rowid WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' AND f.value MATCH ? ORDER BY f.rank LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), fullTextSearch, count, start)
            )
        } else {
            val sortOrder = if (reverseSort) "DESC" else "ASC"
            db.rawQuery(
                "SELECT l.path, d.path, d.value FROM ${schema}_data l INNER JOIN ${schema}_data d ON l.value = d.path WHERE l.path LIKE ? AND SUBSTR(CAST(l.value AS TEXT), 1, 1) = 'T' ORDER BY l.path $sortOrder LIMIT ? OFFSET ?",
                arrayOf(serde.serialize(pathQuery), count, start)
            )
        }
        cursor.use {
            if (cursor != null) {
                while (cursor.moveToNext()) {
                    onResult(
                        serde.deserialize(cursor.getBlob(0)),
                        serde.deserialize(cursor.getBlob(1)),
                        cursor.getBlob(2)
                    )
                }
            }
        }
    }
}

class Transaction internal constructor(
    db: SQLiteDatabase,
    schema: String,
    serde: Serde,
    internal val subscribersBySchema: HashMap<String, RadixTree<PersistentMap<String, RawSubscriber<Any>>>>,
    private val parentUpdatesBySchema: HashMap<String, HashMap<String, Raw<Any>>>? = null,
    private val parentDeletionsBySchema: HashMap<String, TreeSet<String>>? = null,
    private val savepoint: String? = null,
) : Queryable(db, schema, serde) {
    internal val updatesBySchema = HashMap<String, HashMap<String, Raw<Any>>>()
    internal val deletionsBySchema = HashMap<String, TreeSet<String>>()
    private var savepointSuccessful = false

    init {
        updatesBySchema[schema] = updatesBySchema[schema] ?: HashMap()
        deletionsBySchema[schema] = deletionsBySchema[schema] ?: TreeSet()
    }

    internal fun beginSavepoint() {
        db.execSQL("SAVEPOINT $savepoint")
    }

    internal fun setSavepointSuccessful() {
        savepointSuccessful = true
    }

    internal fun endSavepoint() {
        db.execSQL(if (savepointSuccessful) "RELEASE $savepoint" else "ROLLBACK TO $savepoint")
        if (savepointSuccessful) {
            // merge updates and deletions into parent
            parentUpdatesBySchema?.let { parentUpdates ->
                updatesBySchema.forEach { (schema, updates) ->
                    val parentUpdatesForSchema = parentUpdates[schema] ?: HashMap()
                    parentUpdatesForSchema.putAll(updates)
                    parentUpdates[schema] = parentUpdatesForSchema
                }
            }
            parentDeletionsBySchema?.let { parentDeletions ->
                deletionsBySchema.forEach { (schema, deletions) ->
                    val parentDeletionsForSchema = parentDeletions[schema] ?: TreeSet()
                    parentDeletionsForSchema.addAll(deletions)
                    parentDeletions[schema] = parentDeletionsForSchema
                }
            }
        }
    }

    /**
     * Puts the given value at the given path. If the value is null, the path is deleted. If there's
     * an existing value at this path, it's replaced.
     *
     * If fullText is populated, the given data will also be full text indexed.
     *
     * @return the value that was put
     */
    fun <T : Any?> put(path: String, value: T, fullText: String? = null): T {
        value?.let {
            doPut(path, value, fullText, true)
        } ?: run {
            delete(path)
        }
        return value
    }

    /**
     * Gets the given value if present otherwise puts the new value.
     *
     * @return whatever value is now in the database (either the one gotten or the one put)
     */
    fun <T : Any> getOrPut(path: String, value: T?, fullText: String? = null): T? {
        return get(path) ?: put(path, value, fullText)
    }

    /**
     * Puts the given value at the given path if and only if there was no value already present.
     *
     * @return true if the value was successfully put, false if it wasn't because there was already a value
     */
    fun putIfAbsent(path: String, value: Any, fullText: String? = null): Boolean {
        return try {
            doPut(path, value, fullText, false)
            true
        } catch (e: SQLiteConstraintException) {
            false
        }
    }

    private fun doPut(
        path: String,
        value: Any,
        fullText: String?,
        updateIfPresent: Boolean
    ) {
        val serializedPath = serde.serialize(path)
        val onConflictClause =
            if (updateIfPresent) " ON CONFLICT(path) DO UPDATE SET value = EXCLUDED.value" else ""
        val bytes = serde.serialize(value)
        var rowId: Long? = null
        if (fullText != null) {
            db.execSQL("INSERT INTO ${schema}_counters(id, value) VALUES(0, 0) ON CONFLICT(id) DO UPDATE SET value = value+1")
            db.rawQuery("SELECT value FROM ${schema}_counters WHERE id = 0", null).use { cursor ->
                if (cursor == null || !cursor.moveToNext()) {
                    throw RuntimeException("Unable to read counter value for full text indexing")
                }
                rowId = cursor.getLong(0)
            }
        }
        db.execSQL(
            "INSERT INTO ${schema}_data(path, value, rowid) VALUES(?, ?, ?)$onConflictClause",
            arrayOf(serializedPath, bytes, rowId)
        )
        if (fullText != null) {
            db.execSQL(
                "INSERT INTO ${schema}_fts(rowid, value) VALUES(?, ?)",
                arrayOf(rowId, fullText)
            )
        }
        updatesBySchema[schema]!![path] = Raw(serde, bytes, value)
        deletionsBySchema[schema]!! -= path
    }

    /**
     * Puts all path/value pairs into the db
     */
    fun putAll(map: Map<String, Any?>) {
        map.forEach { (path, value) -> put(path, value) }
    }

    /**
     * Deletes the value at the given path
     */
    fun <T : Any> delete(path: String, extractFullText: ((T) -> String)? = null) {
        val serializedPath = serde.serialize(path)
        extractFullText?.let {
            db.rawQuery(
                "SELECT rowid, value FROM ${schema}_data WHERE path = ?", arrayOf(serializedPath)
            ).use { cursor ->
                if (cursor != null && cursor.moveToNext()) {
                    db.execSQL(
                        "INSERT INTO ${schema}_fts(${schema}_fts, rowid, value) VALUES('delete', ?, ?)",
                        arrayOf(
                            cursor.getLong(0),
                            extractFullText(serde.deserialize(cursor.getBlob(1)))
                        )
                    )
                }
            }
        }
        db.execSQL("DELETE FROM ${schema}_data WHERE path = ?", arrayOf(serializedPath))
        deletionsBySchema[schema]!! += path
        updatesBySchema[schema]!!.remove(path)
    }

    fun delete(path: String) {
        delete<Any>(path, null)
    }

    internal fun publish() {
        val changesBySubscriber = HashMap<RawSubscriber<Any>, RawChangeSet<Any>>()

        updatesBySchema.forEach { (schema, updates) ->
            updates.forEach { (path, newValue) ->
                subscribersBySchema[schema]?.visit(object :
                        RadixTreeVisitor<PersistentMap<String, RawSubscriber<Any>>, Void?> {
                        override fun visit(
                            key: String?,
                            value: PersistentMap<String, RawSubscriber<Any>>?
                        ): Boolean {
                            if (key == null || !path.startsWith(key)) {
                                return false
                            }
                            value?.values?.forEach {
                                var changes = changesBySubscriber[it]
                                if (changes == null) {
                                    changes = RawChangeSet(updates = HashMap(), deletions = HashSet())
                                    changesBySubscriber[it] = changes
                                }
                                (changes.updates as MutableMap)[path] = newValue
                            }
                            return true
                        }

                        override fun getResult(): Void? {
                            return null
                        }
                    })
            }
        }

        deletionsBySchema.forEach { (schema, deletions) ->
            deletions.forEach { path ->
                subscribersBySchema[schema]?.visit(object :
                        RadixTreeVisitor<PersistentMap<String, RawSubscriber<Any>>, Void?> {
                        override fun visit(
                            key: String?,
                            value: PersistentMap<String, RawSubscriber<Any>>?
                        ): Boolean {
                            if (key == null || !path.startsWith(key)) {
                                return false
                            }
                            value?.values?.forEach {
                                var changes = changesBySubscriber[it]
                                if (changes == null) {
                                    changes = RawChangeSet(updates = HashMap(), deletions = HashSet())
                                    changesBySubscriber[it] = changes!!
                                }
                                (changes!!.deletions as MutableSet).add(path)
                            }
                            return true
                        }

                        override fun getResult(): Void? {
                            return null
                        }
                    })
            }
        }

        changesBySubscriber.forEach { (subscriber, changes) ->
            subscriber.onChanges(changes)
        }
    }
}
