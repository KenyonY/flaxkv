# API reference

This document is the API reference for Plyvel. It describes all classes, methods, functions, and attributes that are part of the public API.

Most of the terminology in the Plyvel API comes straight from the LevelDB API. See the LevelDB documentation and the LevelDB header files (`$prefix/include/leveldb/*.h`) for more detailed explanations of all flags and options.

## Database

Plyvel exposes the `DB` class as the main interface to LevelDB. Application code should create a `DB` and use the appropriate methods on this instance to create write batches, snapshots, and iterators for that LevelDB database.
*class*DB

LevelDB database
__init__(*name*, *create_if_missing=False*, *error_if_exists=False*, *paranoid_checks=None*, *write_buffer_size=None*, *max_open_files=None*, *lru_cache_size=None*, *block_size=None*, *block_restart_interval=None*, *max_file_size=None*, *compression='snappy'*, *bloom_filter_bits=0*, *comparator=None*, *comparator_name=None*)

Open the underlying database handle.

Most arguments have the same name as the corresponding LevelDB parameters; see the LevelDB documentation for a detailed description. Arguments defaulting to None are only propagated to LevelDB if specified, e.g. not specifying a write_buffer_size means the LevelDB defaults are used.

Most arguments are optional; only the database name is required.

Instances of this class can be used as context managers (Python’s `with` block). When the `with` block terminates, the database automatically closes without an explicit call to `DB.close()`.

See the descriptions for `DB`, `DB::Open()`, `Cache`, `FilterPolicy`, and `Comparator` in the LevelDB C++ API for more information.

Added in version 1.0.0: max_file_size argument
Parameters:
- 

**name** (*str*) – name of the database (directory name)

- 

**create_if_missing** (*bool*) – whether a new database should be created if needed

- 

**error_if_exists** (*bool*) – whether to raise an exception if the database already exists

- 

**paranoid_checks** (*bool*) – whether to enable paranoid checks

- 

**write_buffer_size** (*int*) – size of the write buffer (in bytes)

- 

**max_open_files** (*int*) – maximum number of files to keep open

- 

**lru_cache_size** (*int*) – size of the LRU cache (in bytes)

- 

**block_size** (*int*) – block size (in bytes)

- 

**block_restart_interval** (*int*) – block restart interval for delta encoding of keys

- 

**max_file_size** (*bool*) – maximum file size (in bytes)

- 

**compression** (*bool*) – whether to use Snappy compression (enabled by default))

- 

**bloom_filter_bits** (*int*) – the number of bits to use per key for a bloom filter; the default of 0 means that no bloom filter will be used

- 

**comparator** (*callable*) – a custom comparator callable that takes two byte strings and returns an integer

- 

**comparator_name** (*bytes*) – name for the custom comparator

name

The (directory) name of this `DB` instance. This is a *read-only* attribute and must be set at instantiation time.

*New in version 1.1.0.*
close()

Close the database.

This closes the database and releases associated resources such as open file pointers and caches.

Any further operations on the closed database will raise `RuntimeError`.

Warning

Closing a database while other threads are busy accessing the same database may result in hard crashes, since database operations do not perform any synchronisation/locking on the database object (for performance reasons) and simply assume it is available (and open). Applications should make sure not to close databases that are concurrently used from other threads.

See the description for `DB` in the LevelDB C++ API for more information. This method deletes the underlying DB handle in the LevelDB C++ API and also frees other related objects.
closed

Boolean attribute indicating whether the database is closed.
get(*key*, *default=None*, *verify_checksums=False*, *fill_cache=True*)

Get the value for the specified key, or default if no value was set.

See the description for `DB::Get()` in the LevelDB C++ API for more information.

Added in version 0.4: default argument
Parameters:
- 

**key** (*bytes*) – key to retrieve

- 

**default** – default value if key is not found

- 

**verify_checksums** (*bool*) – whether to verify checksums

- 

**fill_cache** (*bool*) – whether to fill the cache

Returns:

value for the specified key, or None if not found
Return type:

bytes
put(*key*, *value*, *sync=False*)

Set a value for the specified key.

See the description for `DB::Put()` in the LevelDB C++ API for more information.
Parameters:
- 

**key** (*bytes*) – key to set

- 

**value** (*bytes*) – value to set

- 

**sync** (*bool*) – whether to use synchronous writes

delete(*key*, *sync=False*)

Delete the key/value pair for the specified key.

See the description for `DB::Delete()` in the LevelDB C++ API for more information.
Parameters:
- 

**key** (*bytes*) – key to delete

- 

**sync** (*bool*) – whether to use synchronous writes

write_batch(*transaction=False*, *sync=False*)

Create a new `WriteBatch` instance for this database.

See the `WriteBatch` API for more information.

Note that this method does not write a batch to the database; it only creates a new write batch instance.
Parameters:
- 

**transaction** (*bool*) – whether to enable transaction-like behaviour when the batch is used in a `with` block

- 

**sync** (*bool*) – whether to use synchronous writes

Returns:

new `WriteBatch` instance
Return type:

`WriteBatch`
iterator(*reverse=False*, *start=None*, *stop=None*, *include_start=True*, *include_stop=False*, *prefix=None*, *include_key=True*, *include_value=True*, *verify_checksums=False*, *fill_cache=True*)

Create a new `Iterator` instance for this database.

All arguments are optional, and not all arguments can be used together, because some combinations make no sense. In particular:

- 

start and stop cannot be used if a prefix is specified.

- 

include_start and include_stop are only used if start and stop are specified.

Note: due to the way the prefix support is implemented, this feature only works reliably when the default DB comparator is used.

See the `Iterator` API for more information about iterators.
Parameters:
- 

**reverse** (*bool*) – whether the iterator should iterate in reverse order

- 

**start** (*bytes*) – the start key (inclusive by default) of the iterator range

- 

**stop** (*bytes*) – the stop key (exclusive by default) of the iterator range

- 

**include_start** (*bool*) – whether to include the start key in the range

- 

**include_stop** (*bool*) – whether to include the stop key in the range

- 

**prefix** (*bytes*) – prefix that all keys in the the range must have

- 

**include_key** (*bool*) – whether to include keys in the returned data

- 

**include_value** (*bool*) – whether to include values in the returned data

- 

**verify_checksums** (*bool*) – whether to verify checksums

- 

**fill_cache** (*bool*) – whether to fill the cache

Returns:

new `Iterator` instance
Return type:

`Iterator`
raw_iterator(*verify_checksums=False*, *fill_cache=True*)

Create a new `RawIterator` instance for this database.

See the `RawIterator` API for more information.
snapshot()

Create a new `Snapshot` instance for this database.

See the `Snapshot` API for more information.
get_property(*name*)

Get the specified property from LevelDB.

This returns the property value or None if no value is available. Example property name: `b'leveldb.stats'`.

See the description for `DB::GetProperty()` in the LevelDB C++ API for more information.
Parameters:

**name** (*bytes*) – name of the property
Returns:

property value or None
Return type:

bytes
compact_range(*start=None*, *stop=None*)

Compact underlying storage for the specified key range.

See the description for `DB::CompactRange()` in the LevelDB C++ API for more information.
Parameters:
- 

**start** (*bytes*) – start key of range to compact (optional)

- 

**stop** (*bytes*) – stop key of range to compact (optional)

approximate_size(*start*, *stop*)

Return the approximate file system size for the specified range.

See the description for `DB::GetApproximateSizes()` in the LevelDB C++ API for more information.
Parameters:
- 

**start** (*bytes*) – start key of the range

- 

**stop** (*bytes*) – stop key of the range

Returns:

approximate size
Return type:

int
approximate_sizes(*\*ranges*)

Return the approximate file system sizes for the specified ranges.

This method takes a variable number of arguments. Each argument denotes a range as a (start, stop) tuple, where start and stop are both byte strings. Example:

```
db.approximate_sizes(
    (b'a-key', b'other-key'),
    (b'some-other-key', b'yet-another-key'))

```

See the description for `DB::GetApproximateSizes()` in the LevelDB C++ API for more information.
Parameters:

**ranges** – variable number of (start, stop) tuples
Returns:

approximate sizes for the specified ranges
Return type:

list
prefixed_db(*prefix*)

Return a new `PrefixedDB` instance for this database.

See the `PrefixedDB` API for more information.
Parameters:

**prefix** (*bytes*) – prefix to use
Returns:

new `PrefixedDB` instance
Return type:

`PrefixedDB`

### Prefixed database
*class*PrefixedDB

A `DB`-like object that transparently prefixes all database keys.

Do not instantiate directly; use `DB.prefixed_db()` instead.
prefix

The prefix used by this `PrefixedDB`.
db

The underlying `DB` instance.
get(*...*)

See `DB.get()`.
put(*...*)

See `DB.put()`.
delete(*...*)

See `DB.delete()`.
write_batch(*...*)

See `DB.write_batch()`.
iterator(*...*)

See `DB.iterator()`.
snapshot(*...*)

See `DB.snapshot()`.
prefixed_db(*...*)

Create another `PrefixedDB` instance with an additional key prefix, which will be appended to the prefix used by this `PrefixedDB` instance.

See `DB.prefixed_db()`.

### Database maintenance

Existing databases can be repaired or destroyed using these module level functions:
repair_db(*name*, *paranoid_checks=None*, *write_buffer_size=None*, *max_open_files=None*, *lru_cache_size=None*, *block_size=None*, *block_restart_interval=None*, *max_file_size=None*, *compression='snappy'*, *bloom_filter_bits=0*, *comparator=None*, *comparator_name=None*)

Repair the specified database.

See `DB` for a description of the arguments.

See the description for `RepairDB()` in the LevelDB C++ API for more information.
destroy_db(*name*)

Destroy the specified database.
Parameters:

**name** (*str*) – name of the database (directory name)

See the description for `DestroyDB()` in the LevelDB C++ API for more information.

## Write batch
*class*WriteBatch

Write batch for batch put/delete operations

Instances of this class can be used as context managers (Python’s `with` block). When the `with` block terminates, the write batch will automatically write itself to the database without an explicit call to `WriteBatch.write()`:

```
with db.write_batch() as b:
    b.put(b'key', b'value')

```

The transaction argument to `DB.write_batch()` specifies whether the batch should be written after an exception occurred in the `with` block. By default, the batch is written (this is like a `try` statement with a `finally` clause), but if transaction mode is enabled`, the batch will be discarded (this is like a `try` statement with an `else` clause).

Note: methods on a `WriteBatch` do not take a sync argument; this flag can be specified for the complete write batch when it is created using `DB.write_batch()`.

Do not instantiate directly; use `DB.write_batch()` instead.

See the descriptions for `WriteBatch` and `DB::Write()` in the LevelDB C++ API for more information.
put(*key*, *value*)

Set a value for the specified key.

This is like `DB.put()`, but operates on the write batch instead.
delete(*key*)

Delete the key/value pair for the specified key.

This is like `DB.delete()`, but operates on the write batch instead.
clear()

Clear the batch.

This discards all updates buffered in this write batch.
write()

Write the batch to the associated database. If you use the write batch as a context manager (in a `with` block), this method will be invoked automatically.
append(*source*)

Copy the operations in source (another `WriteBatch` instance) to this batch.
approximate_size()

Return the size of the database changes caused by this batch.

## Snapshot
*class*Snapshot

Database snapshot

A snapshot provides a consistent view over keys and values. After making a snapshot, puts and deletes on the database will not be visible by the snapshot.

Do not keep unnecessary references to instances of this class around longer than needed, because LevelDB will not release the resources required for this snapshot until a snapshot is released.

Do not instantiate directly; use `DB.snapshot()` instead.

See the descriptions for `DB::GetSnapshot()` and `DB::ReleaseSnapshot()` in the LevelDB C++ API for more information.
get(*...*)

Get the value for the specified key, or None if no value was set.

Same as `DB.get()`, but operates on the snapshot instead.
iterator(*...*)

Create a new `Iterator` instance for this snapshot.

Same as `DB.iterator()`, but operates on the snapshot instead.
raw_iterator(*...*)

Create a new `RawIterator` instance for this snapshot.

Same as `DB.raw_iterator()`, but operates on the snapshot instead.
close()

Close the snapshot. Can also be accomplished using a context manager. See `Iterator.close()` for an example.

Added in version 0.8.
release()

Alias for `Snapshot.close()`. *Release* is the terminology used in the LevelDB C++ API.

Added in version 0.8.

## Iterator

### Regular iterators

Plyvel’s `Iterator` is intended to be used like a normal Python iterator, so you can just use a standard `for` loop to iterate over it. Directly invoking methods on the `Iterator` returned by `DB.iterator()` method is only required for additional functionality.
*class*Iterator

Iterator to iterate over (ranges of) a database

The next item in the iterator can be obtained using the `next()` built-in or by looping over the iterator using a `for` loop.

Do not instantiate directly; use `DB.iterator()` or `Snapshot.iterator()` instead.

See the descriptions for `DB::NewIterator()` and `Iterator` in the LevelDB C++ API for more information.
prev()

Move one step back and return the previous entry.

This returns the same value as the most recent `next()` call (if any).
seek_to_start()

Move the iterator to the start key (or the begin).

This “rewinds” the iterator, so that it is in the same state as when first created. This means calling `next()` afterwards will return the first entry.
seek_to_stop()

Move the iterator to the stop key (or the end).

This “fast-forwards” the iterator past the end. After this call the iterator is exhausted, which means a call to `next()` raises StopIteration, but `prev()` will work.
seek(*target*)

Move the iterator to the specified target.

This moves the iterator to the the first key that sorts equal or after the specified target within the iterator range (start and stop).
close()

Close the iterator.

This closes the iterator and releases the associated resources. Any further operations on the closed iterator will raise `RuntimeError`.

To automatically close an iterator, a context manager can be used:

```
with db.iterator() as it:
    for k, v in it:
        pass  # do something

it.seek_to_start()  # raises RuntimeError

```

Added in version 0.6.

### Raw iterators

The raw iteration API mimics the C++ iterator interface provided by LevelDB. See the LevelDB documentation for a detailed description.
*class*RawIterator

Raw iterator to iterate over a database

Added in version 0.7.
valid()

Check whether the iterator is currently valid.
seek_to_first()

Seek to the first key (if any).
seek_to_last()

Seek to the last key (if any).
seek(*target*)

Seek to or past the specified key (if any).
next()

Move the iterator one step forward.

May raise `IteratorInvalidError`.
prev()

Move the iterator one step backward.

May raise `IteratorInvalidError`.
key()

Return the current key.

May raise `IteratorInvalidError`.
value()

Return the current value.

May raise `IteratorInvalidError`.
item()

Return the current key and value as a tuple.

May raise `IteratorInvalidError`.
close()

Close the iterator. Can also be accomplished using a context manager. See `Iterator.close()`.

## Errors

Plyvel uses standard exceptions like `TypeError`, `ValueError`, and `RuntimeError` as much as possible. For LevelDB specific errors, Plyvel may raise a few custom exceptions, which are described below.
*exception*Error

Generic LevelDB error

This class is also the “parent” error for other LevelDB errors (`IOError` and `CorruptionError`). Other exceptions from this module extend from this class.
*exception*IOError

LevelDB IO error

This class extends both the main LevelDB Error class from this module and Python’s built-in IOError.
*exception*CorruptionError

LevelDB corruption error
*exception*IteratorInvalidError

Used by `RawIterator` to signal invalid iterator state.

# Plyvel

### Navigation

- Installation guide
- User guide
- API reference
  - Database
  - Write batch
  - Snapshot
  - Iterator
  - Errors
- Version history
- Contributing and developing
- License