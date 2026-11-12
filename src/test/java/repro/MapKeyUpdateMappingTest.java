package repro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.core.PropertyReferenceException;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.convert.UpdateMapper;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Update;

/**
 * Maps updates whose key goes through a {@code Map}-typed property, such as {@code queues.<key>.last}, with
 * {@link UpdateMapper} alone. No database is needed.
 */
class MapKeyUpdateMappingTest {

   static class Node {
      @Id String id;
      String name;
      Map<String, State> queues;
   }

   static class State {
      Instant last;
   }

   static class RenamedNode {
      @Id String id;
      @Field("q") Map<String, RenamedState> queues;
   }

   static class RenamedState {
      @Field("l") Instant last;
   }

   private static final MongoMappingContext context = new MongoMappingContext();
   private static final UpdateMapper mapper;

   static {
      MongoCustomConversions conversions = new MongoCustomConversions(List.of());
      context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
      context.setInitialEntitySet(Set.of(Node.class, RenamedNode.class));
      context.afterPropertiesSet();
      MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
      converter.setCustomConversions(conversions);
      converter.afterPropertiesSet();
      mapper = new UpdateMapper(converter);
   }

   /**
    * A real property's path is resolved once, then served from {@code SimplePropertyPath}'s cache. A map key isn't a
    * property of the map's value type, so resolving the path throws, and the failure isn't cached: every update
    * resolves it again. Fails on 5.1.2.
    */
   @Test
   void mapKeyPathIsResolvedOnlyOnce() throws Exception {
      PropertyReferenceException e = assertThrows(PropertyReferenceException.class,
         () -> PropertyPath.from("queues.a.last", Node.class));
      System.out.printf("%nPropertyPath.from(\"queues.a.last\", Node.class) throws: %s%n", e.getMessage());

      int realPropertyGrowth = cacheGrowth(() -> map(Node.class, "name"));
      int realPropertyRepeatGrowth = cacheGrowth(() -> map(Node.class, "name"));
      int mapKeyGrowth = cacheGrowth(() -> map(Node.class, "queues.a.last"));
      int mapKeyRepeatGrowth = cacheGrowth(() -> map(Node.class, "queues.a.last"));
      System.out.printf("SimplePropertyPath cache entries added by mapping 'name': first %d, again %d%n",
         realPropertyGrowth, realPropertyRepeatGrowth);
      System.out.printf("SimplePropertyPath cache entries added by mapping 'queues.a.last': first %d, again %d%n",
         mapKeyGrowth, mapKeyRepeatGrowth);

      assertTrue(mapKeyGrowth > 0, "the map-key path should be resolved once and cached");
   }

   /**
    * As the path doesn't resolve, {@code @Field} names aren't applied to it. A numeric key is read as a position and
    * does resolve, so it is renamed. Fails on 5.1.2.
    */
   @Test
   void fieldNamesApplyToMapKeyPaths() {
      String numericKey = map(RenamedNode.class, "queues.1.last");
      String stringKey = map(RenamedNode.class, "queues.a.last");
      System.out.printf("%n'queues.1.last' maps to '%s'%n'queues.a.last' maps to '%s'%n", numericKey, stringKey);

      assertEquals("q.a.l", stringKey);
   }

   /**
    * Since spring-framework 6.2.13 and 7.0 (gh-35794), a {@code ConcurrentReferenceHashMap.computeIfAbsent} miss runs
    * the mapping function inside the segment's lock. Every map-key update misses, so concurrent ones queue on it. Fails
    * on 5.1.2.
    */
   @Test
   void concurrentMapKeyUpdatesScale() throws Exception {
      int threads = Math.min(16, Runtime.getRuntime().availableProcessors());
      assumeTrue(threads >= 4, "needs at least 4 cores");

      System.out.printf("%n%-28s %8s %14s %38s%n", "Update key", "Threads", "Updates/s", "Waiting in ConcurrentReferenceHashMap");
      run("name", 1, t -> "name");
      run("name", threads, t -> "name");
      Result one = run("queues.a.last", 1, t -> "queues.a.last");
      Result many = run("queues.a.last", threads, t -> "queues.a.last");
      run("queues.<thread>.last", threads, t -> "queues.k" + t + ".last");

      assertTrue(many.perSecond >= one.perSecond,
         "map-key updates on " + threads + " threads should be at least as fast as on 1 thread");
   }

   private record Result(double perSecond, double waitingFraction) {
   }

   /** Maps updates on each thread for 2 s after a 1 s warm-up, sampling what the threads are waiting on. */
   private static Result run(String label, int threads, IntFunction<String> keyForThread) throws Exception {
      AtomicBoolean measuring = new AtomicBoolean();
      AtomicBoolean stop = new AtomicBoolean();
      AtomicLong updates = new AtomicLong();
      List<Thread> workers = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
         String key = keyForThread.apply(t);
         Thread worker = new Thread(() -> {
            while (!stop.get()) {
               map(Node.class, key);
               if (measuring.get()) {
                  updates.incrementAndGet();
               }
            }
         });
         worker.start();
         workers.add(worker);
      }
      Thread.sleep(1000);
      measuring.set(true);
      long start = System.nanoTime();
      long samples = 0;
      long waiting = 0;
      while (System.nanoTime() - start < 2_000_000_000L) {
         Thread.sleep(20);
         for (Thread worker : workers) {
            samples++;
            if (worker.getState() == Thread.State.WAITING && isInConcurrentReferenceHashMap(worker.getStackTrace())) {
               waiting++;
            }
         }
      }
      measuring.set(false);
      double seconds = (System.nanoTime() - start) / 1e9;
      stop.set(true);
      for (Thread worker : workers) {
         worker.join();
      }
      Result result = new Result(updates.get() / seconds, (double) waiting / samples);
      System.out.printf("%-28s %8d %,14.0f %37.0f%%%n", label, threads, result.perSecond, 100 * result.waitingFraction);
      return result;
   }

   private static boolean isInConcurrentReferenceHashMap(StackTraceElement[] stack) {
      for (StackTraceElement frame : stack) {
         if (frame.getClassName().startsWith("org.springframework.util.ConcurrentReferenceHashMap")) {
            return true;
         }
      }
      return false;
   }

   /** Maps {@code $set} of the key, returning the key it maps to. */
   private static String map(Class<?> type, String key) {
      MongoPersistentEntity<?> entity = context.getRequiredPersistentEntity(type);
      Document mapped = mapper.getMappedObject(new Update().set(key, Instant.EPOCH).getUpdateObject(), entity);
      return ((Document) mapped.get("$set")).keySet().iterator().next();
   }

   private static int cacheGrowth(Runnable action) throws Exception {
      int before = propertyPathCache().size();
      action.run();
      return propertyPathCache().size() - before;
   }

   /** {@code SimplePropertyPath}'s static cache of resolved paths, read reflectively as it's package-private. */
   private static Map<?, ?> propertyPathCache() throws Exception {
      java.lang.reflect.Field cache = Class.forName("org.springframework.data.core.SimplePropertyPath")
         .getDeclaredField("cache");
      cache.setAccessible(true);
      return (Map<?, ?>) cache.get(null);
   }
}
