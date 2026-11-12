# Spring Data MongoDB updates through a map key are never cached, and queue on a lock

On an entity with `Map<String, State> queues`, an update such as `new Update().set("queues.<key>.last", value)` fails
to resolve its property path, as the map key isn't a property of `State`. `QueryMapper$MetadataBackedField.forName`
tries `PropertyPath.from` twice, plain and quoted, and both throw `PropertyReferenceException`. `SimplePropertyPath`
caches only paths that resolve, so every such update resolves the path again.

Since spring-framework 6.2.13 and 7.0
([gh-35794](https://github.com/spring-projects/spring-framework/issues/35794)), a
`ConcurrentReferenceHashMap.computeIfAbsent` miss runs the mapping function inside the segment's lock, so concurrent
map-key updates queue on `SimplePropertyPath`'s cache: on 16 threads they're slower than on 1.

As the path doesn't resolve, `@Field` names on the map and on the value's properties aren't applied to it either.

## Run

Needs Java 17+ and Maven. No database is needed.

```
mvn test
```

The tests map `$set` updates with `UpdateMapper` alone:

- `mapKeyPathIsResolvedOnlyOnce` maps `name` and `queues.a.last` twice each, and counts the entries each adds to
  `SimplePropertyPath`'s cache, read reflectively.
- `fieldNamesApplyToMapKeyPaths` maps `queues.1.last` and `queues.a.last` on an entity with `@Field("q")` on the map
  and `@Field("l")` on the value's property.
- `concurrentMapKeyUpdatesScale` maps updates for 2 s on 1 thread and on up to 16, for a real property, a map key
  shared by all threads, and a distinct map key per thread. It samples how often the threads are waiting in
  `ConcurrentReferenceHashMap`. Needs at least 4 cores.

## Expected

- A map-key path is resolved once, then served from a cache, like a real property's.
- `queues.a.last` maps to `q.a.l`, as `queues.1.last` maps to `q.1.l`.
- Map-key updates are at least as fast on 16 threads as on 1.

## Actual

All three tests fail. On 24 cores:

```
'queues.1.last' maps to 'q.1.l'
'queues.a.last' maps to 'queues.a.last'

PropertyPath.from("queues.a.last", Node.class) throws: No property 'a' found for type 'State'; Traversed path: Node.queues
SimplePropertyPath cache entries added by mapping 'name': first 1, again 0
SimplePropertyPath cache entries added by mapping 'queues.a.last': first 0, again 0

Update key                    Threads      Updates/s  Waiting in ConcurrentReferenceHashMap
name                                1      1,620,685                                     0%
name                               16      4,886,701                                     0%
queues.a.last                       1        234,703                                     0%
queues.a.last                      16        158,945                                    91%
queues.<thread>.last               16        692,353                                    61%
```

The last column is the share of samples in which a thread was parked in `ConcurrentReferenceHashMap`. Distinct keys
still contend, as different keys hash to the same one of the map's 16 segments.

## Versions

spring-data-mongodb 5.1.2, spring-data-commons 4.1.2 (Spring Data 2026.0.2), spring-framework 7.0.10, Java 27.
`QueryMapper$MetadataBackedField.forName` is unchanged on `main`.
