## Compression

Cassandra's binary protocol supports optional compression of
transport-level requests and responses, for example:

* a query with its serialized parameters;
* a [page](../paging/) from a result set, i.e. a list of serialized
  rows.

It reduces network traffic at the cost of CPU overhead, therefore it
will likely be beneficial when you have larger payloads.

Two algorithms are available:
[LZ4](https://github.com/jpountz/lz4-java) and
[Snappy](https://code.google.com/p/snappy/).  The LZ4 implementation is a good
first choice; it offers fallback implementations in case native libraries fail
to load and
[benchmarks](http://java-performance.info/performance-general-compression/)
suggest that it offers better performance and compression ratios over Snappy.
Both rely on third-party libraries, declared by the driver as *optional*
dependencies. So if you use a build tool like Maven, you'll need to declare an
explicit dependency to pull the appropriate library in your application's
classpath. Then you configure compression at driver startup.

### LZ4

Maven dependency:

```xml
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.4.1</version>
</dependency>
```

Always check the exact version of the library: go to the driver's
[parent POM][pom] (change the URL to match your driver version) and look
for the `lz4.version` property.

Driver configuration:

```java
cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withCompression(ProtocolOptions.Compression.LZ4)
    .build();
```

LZ4-java has three internal implementations (from fastest to slowest):

* JNI;
* pure Java using sun.misc.Unsafe;
* pure Java using only "safe" classes.

It will pick the best implementation depending on what's possible on
your platform. To find out which one was chosen, [enable INFO
logs](../logging/) on the category
`com.datastax.driver.core.LZ4Compressor` and look for a log similar to
this:

```
INFO  com.datastax.driver.core.LZ4Compressor  - Using LZ4Factory:JNI
```

### Snappy

Maven dependency:

```xml
<dependency>
    <groupId>org.xerial.snappy</groupId>
    <artifactId>snappy-java</artifactId>
    <version>1.1.2.6</version>
</dependency>
```

Always check the exact version of the library: go to the driver's
[parent POM][pom] (change the URL to match your driver version) and look
for the `snappy.version` property.

Driver configuration:

```java
cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withCompression(ProtocolOptions.Compression.SNAPPY)
    .build();
```

[pom]: https://repo1.maven.org/maven2/com/scylladb/scylla-driver-parent/3.11.0.0/scylla-driver-parent-3.11.0.0.pom
