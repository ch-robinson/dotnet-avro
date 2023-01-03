Chr.Avro was created as a flexible alternative to Apache’s C# Avro implementation. This document explains the rationale for the creation of an entirely new library and outlines some of the differences between Chr.Avro and other implementations.

## Schema representation

The main architectural difference between Chr.Avro and other implementations is its abstract schema representation. One of the main drawbacks of the Apache implementation is that its schema respresentations are inextricably bound to JSON. The [`Parse`](https://avro.apache.org/docs/current/api/csharp/html/classAvro_1_1Schema.html#a4630c3ad0c02c3cb7c6f41d27cccb47a) method on the [`Schema`](https://avro.apache.org/docs/current/api/csharp/html/classAvro_1_1Schema.html) class is the only publicly exposed factory method, which means that it’s impossible to manipulate a schema without manipulating JSON. Chr.Avro keeps its abstract, binary, and JSON components entirely separate.

## Development activity

The Apache implementation is minimally maintained. Small changes are contributed occasionally; the last major changes were [years ago](https://github.com/apache/avro/commits/master/lang/csharp/src/apache). There doesn’t appear to be any appetite for major changes. [Microsoft.Hadoop.Avro](https://azure.microsoft.com/en-us/blog/microsoft-avro-library/) ([Microsoft.Avro.Core](https://github.com/dougmsft/microsoft-avro)?) was the only other open source Avro implementation for .NET. It’s been abandoned since 2016.

## Type mapping

The Microsoft implementation made it extremely easy to map Avro records to existing .NET classes, something that Chr.Avro has aimed to imitate. The Apache implementation does not map to existing classes. Instead, users are given a choice between two less flexible options:

*   Use the [`GenericRecord` class](https://avro.apache.org/docs/current/api/csharp/html/classAvro_1_1Generic_1_1GenericRecord.html), essentially an untyped dictionary. This approach offers no compile-time guarantees.

*   Use the [avrogen](https://github.com/confluentinc/confluent-kafka-dotnet#working-with-apache-avro) tool to generate classes that implement [`ISpecificRecord`](https://avro.apache.org/docs/current/api/csharp/html/interfaceAvro_1_1Specific_1_1ISpecificRecord.html). While the generated classes offer some compile-time safety, the process is cumbersome, and additional work usually has to be done to map the generated classes to actual model classes.

## Undefined behaviors

The Avro specification leaves certain behaviors undefined, and in some cases Chr.Avro implements them differently than other libraries. None of these differences are correctness issues—all serialized payloads are correct, and all correct payloads can be deserialized.

### Block sizes

Avro encodes arrays and maps as a series of blocks terminated by an empty block. For example, an array of length 20 could be encoded as 4 blocks with lengths 6, 10, 4, and 0. Chr.Avro doesn’t make any effort to break arrays and maps into chunks; instead, it always encodes all non-empty arrays and maps as two blocks (the first full-length, the second zero-length). This is consistent with most other implementations.

### Invalid boolean values

Avro specifies that booleans should be encoded as a single byte: `0x00` (false) or `0x01` (true). If a value greater than `0x01` is encountered, Chr.Avro decodes the value as true.

The [Apache Java implementation](https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/io/BinaryDecoder.java#L133) decodes all non-`0x01` values as false. The [Apache C# implementation](https://github.com/apache/avro/blob/master/lang/csharp/src/apache/main/IO/BinaryDecoder.cs#L53) throws an exception if a value other than `0x00` or `0x01` is encountered.
