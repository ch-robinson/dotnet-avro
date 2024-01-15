Chr.Avro supports mapping .NET’s [built-in types](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/built-in-types-table), as well as commonly used types like <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetime">DateTime</a></code> and <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.uri">Uri</a></code>, to Avro schemas. This document is a comprehensive explanation of how that mapping works.

The serializer builder and deserializer builder generally throw `UnsupportedTypeException` when a type can’t be mapped to a schema. Other exceptions, usually <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.overflowexception">OverflowException</a></code> and <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code>, are thrown when errors occur during serialization or deserialization.

## Arrays

Avro specifies an `"array"` type for variable-length lists of items. Chr.Avro can map a .NET type to `"array"` if any of the following is true:

1.  The type is a one-dimensional or jagged array. Multi-dimensional arrays are currently not supported because they can’t be deserialized reliably.
2.  The type is an <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.arraysegment-1">ArraySegment</a>&lt;T&gt;</code> type, a <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.objectmodel.collection-1">Collection</a>&lt;T&gt;</code> type, or a generic collection type from [System.Collections.Generic](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic) or [System.Collections.Immutable](https://learn.microsoft.com/en-us/dotnet/api/system.collections.immutable).
3.  The type implements <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;T&gt;</code> (for serialization) and has a constructor with a single <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;T&gt;</code> parameter (for deserialization).

Some examples:

<table>
  <thead>
    <tr valign="top">
      <th>.NET&nbsp;type</th>
      <th>Serializable</th>
      <th>Deserializable</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[]</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[]</code> is a one-dimensional array type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[,,]</code></td>
      <td align="center"><span role="img" aria-label="not serializable">🚫</span></td>
      <td align="center"><span role="img" aria-label="not deserializable">🚫</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[,,]</code> is a multi-dimensional array type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[][]</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[][]</code> is a jagged array type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;T&gt;</code> is a generic collection type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.iset-1">ISet</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.iset-1">ISet</a>&lt;T&gt;</code> is a generic collection type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1">List</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1">List</a>&lt;T&gt;</code> is a generic collection type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1">List</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[]&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.list-1">List</a>&lt;T&gt;</code> is a generic collection type and <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>[]</code> is an array type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.immutable.immutablequeue-1">ImmutableQueue</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.immutable.immutablequeue-1">ImmutableQueue</a>&lt;T&gt;</code> is a generic collection type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.array">Array</a></code></td>
      <td align="center"><span role="img" aria-label="not serializable">🚫</span></td>
      <td align="center"><span role="img" aria-label="not deserializable">🚫</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.array">Array</a></code> isn’t a generic type, so Chr.Avro can’t determine how to handle its items.</td>
    </tr>
  </tbody>
</table>

## Booleans

Chr.Avro maps Avro’s `"boolean"` primitive type to the .NET <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.boolean">bool</a></code> type. No implicit conversions exist between <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.boolean">bool</a></code> and other types (see the [.NET docs](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/bool#conversions)), so no other mappings are supported.

## Byte arrays

In addition to <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.byte">byte</a>[]</code>, Chr.Avro supports mapping the following types to `"bytes"` and `"fixed"`:

<table>
  <thead>
    <tr valign="top">
      <th>.NET&nbsp;type</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid">Guid</a></code></td>
      <td>
        The <a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid.tobytearray"><code>Guid.ToByteArray</code> method</a> is used for serialization, and the <a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid.-ctor"><code>Guid</code> constructor</a> is used for deserialization. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.argumentexception">ArgumentException</a></code> is thrown when the length is not 16.
      </td>
    </tr>
  </tbody>
</table>

## Dates and times

The Avro spec defines six logical types for temporal data:

*   calendar dates with no time or time zone (`"date"`)
*   duration comprised of months, days, and milliseconds (`"duration"`)
*   times of day with no date or time zone (`"time-millis"` and `"time-micros"`)
*   instants in time (`"timestamp-millis"` and `"timestamp-micros"`)

In addition to the conversions described later in this section, these logical types can be treated as their underlying primitive types:

```csharp
var schema = new MillisecondTimestampSchema();
var serializer = new BinarySerializerBuilder().BuildSerializer<DateTime>();
var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<long>();

var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
deserializer.Deserialize(serializer.Serialize(epoch)); // 0L
```

### Calendar dates

.NET’s <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.dateonly">DateOnly</a></code> struct represents a calendar date with no time or time zone. To match other temporal types, Chr.Avro prefers to map <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.dateonly">DateOnly</a></code>s to [ISO 8601 strings](https://en.wikipedia.org/wiki/ISO_8601#Calendar_dates), avoiding `"date"` by default when building schemas. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code> is thrown when deserializing a value that cannot be parsed.

Serializing and deserializing `"date"` values is supported with the caveat that attempting to deserialize a date before 0001-01-01 or after 9999-12-31 will cause an <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.overflowexception">OverflowException</a></code>.

### Durations

.NET’s <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timespan">TimeSpan</a></code> struct is commonly used to represent durations. Under the hood, a <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timespan">TimeSpan</a></code> stores its value as a number of ticks (1 tick = 100 ns = 0.0001 ms). However, `"duration"` values can’t be reliably converted to a number of ticks; there isn’t a consistent number of milliseconds in a day or a consistent number of days in a month. Also, all three components are represented as unsigned integers, so negative durations cannot be expressed.

In light of those incompatibilities, Chr.Avro prefers to map <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timespan">TimeSpan</a></code>s to [ISO 8601 strings](https://en.wikipedia.org/wiki/ISO_8601#Durations), avoiding `"duration"` by default when building schemas. <a href="https://docs.microsoft.com/en-us/dotnet/api/system.xml.xmlconvert.tostring"><code>XmlConvert.ToString</code></a> and <a href="https://docs.microsoft.com/en-us/dotnet/api/system.xml.xmlconvert.totimespan"><code>XmlConvert.ToTimeSpan</code></a> are used for conversions. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code> is thrown when a string cannot be parsed.

Serializing and deserializing `"duration"` values still work, though there are some limitations. .NET assumes a [consistent number of milliseconds in a day](https://docs.microsoft.com/en-us/dotnet/api/system.timespan.ticksperday), so Chr.Avro supports the day and millisecond components. (This may lead to minor arithmetic inconsistencies with other platforms.) All non-negative <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timespan">TimeSpan</a></code>s can be serialized without using the months component. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.overflowexception">OverflowException</a></code> is thrown when serializing a negative <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timespan">TimeSpan</a></code> and when deserializing a value with a non-zero months component.

### Times

.NET’s <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timeonly">TimeOnly</a></code> struct represents a time of day with no time zone. To match other temporal types, Chr.Avro prefers to map <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timeonly">TimeOnly</a></code>s to [ISO 8601 strings](https://en.wikipedia.org/wiki/ISO_8601#Times), avoiding `"time-millis"` and `"time-micros"` by default when building schemas. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code> is thrown when deserializing a value that cannot be parsed.

Serializing and deserializing `"time-millis"` and `"time-micros"` values is supported. However, .NET date types are tick-precision, so serializing to `"time-millis"` or deserializing from `"time-micros"` may result in a loss of precision.

### Timestamps

Both <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetime">DateTime</a></code> and <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetimeoffset">DateTimeOffset</a></code> can be used to represent timestamps. Chr.Avro prefers to map those types to [ISO 8601 strings](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations), avoiding `"timestamp-millis"` and `"timestamp-micros"` by default when building schemas. This behavior is consistent with how durations are handled, and it also means that <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetime">DateTime</a></code> kind and timezone are retained—the [round-trip (“O”, “o”) format specifier](https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings#Roundtrip) is used for serialization. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code> is thrown when a string cannot be parsed.
Serializing and deserializing `"timestamp-millis"` and `"timestamp-micros"` values are supported as well, with a few caveats:

*   All <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetime">DateTime</a></code>s are converted to UTC. Don’t use <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetime">DateTime</a></code>s with kind unspecified.
*   .NET date types are tick-precision, so serializing to `"timestamp-millis"` or deserializing from `"timestamp-micros"` may result in a loss of precision.

## Enums

Chr.Avro maps .NET enumerations to Avro’s `"enum"` type by matching each symbol on the schema to an enumerator on the enumeration according to these rules:

*   Enumerator names don’t need to be an exact match—all non-alphanumeric characters are stripped and comparisons are case-insensitive. For example, a `PRIMARY_RESIDENCE` symbol will match enumerators named `PrimaryResidence`, `primaryResidence`, etc.
*   When the serializer builder and deserializer builder find multiple matching enumerators, `UnsupportedTypeException` is thrown.
*   When the deserializer builder can’t find a matching enumerator but a default is specified by the schema, the deserializer builder will attempt to map the default instead.
*   When the deserializer builder can’t find a matching enumerator and no default is specified by the schema, `UnsupportedTypeException` is thrown.

By default, Chr.Avro also honors data contract attributes if a <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.runtime.serialization.datacontractattribute">DataContractAttribute</a></code> is present on the enumeration. In that case, if <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.runtime.serialization.enummemberattribute.value">Value</a></code> is set on an enumerator, the custom value must match the symbol exactly. If it’s not set, the enumerator name will be compared inexactly as described above.

To change or extend this behavior, implement `ITypeResolver` or extend one of the existing resolvers (`ReflectionResolver` and `DataContractResolver`).

Because `"enum"` symbols are represented as strings, Chr.Avro also supports mapping enum schemas to <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a></code>. On serialization, if the name of the enumerator is not a symbol in the schema, <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.argumentexception">ArgumentException</a></code> will be thrown.

## Maps

Avro’s `"map"` type represents a map of keys (assumed to be strings) to values. Chr.Avro can map a .NET type to `"map"` if any of the following is true:

1.  The type is a generic dictionary type from [System.Collections.Generic](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic) or [System.Collections.Immutable](https://learn.microsoft.com/en-us/dotnet/api/system.collections.immutable).
2.  The type implements <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.keyvaluepair-2">KeyValuePair</a>&lt;TKey, TValue&gt;&gt;</code> (for serialization) and has a constructor with a single <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.keyvaluepair-2">KeyValuePair</a>&lt;TKey, TValue&gt;&gt;</code> parameter (for deserialization).

Additionally, because Avro map keys are assumed to be strings, serializers and deserializers are built for key types by mapping to `"string"` implicitly.

Some examples of this behavior:

<table>
  <thead>
    <tr valign="top">
      <th>.NET&nbsp;type</th>
      <th>Serializable</th>
      <th>Deserializable</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary-2">IDictionary</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary-2">IDictionary</a>&lt;TKey, TValue&gt;</code> is a generic dictionary type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.dictionary-2">Dictionary</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.dictionary-2">Dictionary</a>&lt;TKey, TValue&gt;</code> is a generic dictionary type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary-2">IDictionary</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid">Guid</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary-2">IDictionary</a>&lt;TKey, TValue&gt;</code> is a generic dictionary type, and <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid">Guid</a></code> can be mapped to <code>"string"</code>.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary{system.byte">IDictionary</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.byte">byte</a>[], <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="not serializable">🚫</span></td>
      <td align="center"><span role="img" aria-label="not deserializable">🚫</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.idictionary{system.byte">IDictionary</a>&lt;TKey, TValue&gt;</code> is a generic dictionary type, but <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.byte">byte</a></code> cannot be mapped to <code>"string"</code>.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.keyvaluepair-2">KeyValuePair</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.keyvaluepair-2">KeyValuePair</a>&lt;TKey, TValue&gt;&gt;</code> is recognized as a generic dictionary type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.icollection-1">ICollection</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.keyvaluepair-2">KeyValuePair</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.icollection-1">ICollection</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.keyvaluepair-2">KeyValuePair</a>&lt;TKey, TValue&gt;&gt;</code> is recognized as a generic dictionary type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.immutable.immutablesorteddictionary-2">ImmutableSortedDictionary</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;</code></td>
      <td align="center"><span role="img" aria-label="serializable">✅</span></td>
      <td align="center"><span role="img" aria-label="deserializable">✅</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.immutable.immutablesorteddictionary-2">ImmutableSortedDictionary</a>&lt;TKey, TValue&gt;</code> is a generic dictionary type.</td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.valuetuple-2">ValueTuple</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a>, <a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a>&gt;&gt;</code></td>
      <td align="center"><span role="img" aria-label="not serializable">🚫</span></td>
      <td align="center"><span role="img" aria-label="not deserializable">🚫</span></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.collections.generic.ienumerable-1">IEnumerable</a>&lt;<a href="https://docs.microsoft.com/en-us/dotnet/api/system.valuetuple-2">ValueTuple</a>&lt;T1, T2&gt;&gt;</code> is not recognized as a generic dictionary type.</td>
    </tr>
  </tbody>
</table>

## Numbers

The Avro spec defines [four primitive numeric types](https://avro.apache.org/docs/current/spec.html#schema_primitive):

*   32-bit signed integers (`"int"`)
*   64-bit signed integers (`"long"`)
*   single-precision (32-bit) floating-point numbers (`"float"`)
*   double-precision (64-bit) floating-point numbers (`"double"`)

It also defines a logical [`"decimal"`](https://avro.apache.org/docs/current/spec.html#Decimal) type that supports arbitrary-precision decimal numbers.

### Integral types

When generating a schema, Chr.Avro maps integral types less than or equal to 32 bits to `"int"` and integral types greater than 32 bits to `"long"`:

<table>
  <thead>
    <tr valign="top">
      <th>.NET&nbsp;type</th>
      <th>Range</th>
      <th align="right">Bits</th>
      <th>Generated&nbsp;schema</th>
    </tr>
  </thead>
  <tbody>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.sbyte">sbyte</a></code></td>
      <td>−128 to 127</td>
      <td align="right">8</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.byte">byte</a></code></td>
      <td>0 to 255</td>
      <td align="right">8</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int16">short</a></code></td>
      <td>−32,768 to 32,767</td>
      <td align="right">16</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.uint16">ushort</a></code></td>
      <td>0 to 65,535</td>
      <td align="right">16</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.char">char</a></code></td>
      <td>0 (U+0000) to 65,535 (U+ffff)</td>
      <td align="right">16</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code></td>
      <td>−2,147,483,648 to 2,147,483,647</td>
      <td align="right">32</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.uint32">uint</a></code></td>
      <td>0 to 4,294,967,295</td>
      <td align="right">32</td>
      <td><code>"int"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int64">long</a></code></td>
      <td>−9,223,372,036,854,775,808 to 9,223,372,036,854,775,807</td>
      <td align="right">64</td>
      <td><code>"long"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.uint64">ulong</a></code></td>
      <td>0 to 18,446,744,073,709,551,615</td>
      <td align="right">64</td>
      <td><code>"long"</code></td>
    </tr>
  </tbody>
</table>

Whether a schema is `"int"` or `"long"` has no impact on serialization. Integers are [zig-zag encoded](https://avro.apache.org/docs/current/spec.html#binary_encoding), so they take up as much space as they need. For that reason, Chr.Avro imposes no constraints on which numeric types can be serialized or deserialized to `"int"` or `"long"`—if a conversion exists, the binary serializer and deserializer will use it.
Because enum types are able to be implicitly converted to and from integral types, Chr.Avro can map any enum type to `"int"` or `"long"` as well.

### Non-integral types

On the non-integral side, .NET types are mapped to their respective Avro types:

<table>
  <thead>
    <tr valign="top">
      <th>.NET&nbsp;type</th>
      <th>Approximate range</th>
      <th>Precision</th>
      <th>Generated schema</th>
    </tr>
  </thead>
  <tbody>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.single">float</a></code></td>
      <td>±1.5&nbsp;×&nbsp;10<sup>−45</sup> to ±3.4&nbsp;×&nbsp;10<sup>38</sup></td>
      <td>~6–9 digits</td>
      <td><code>"float"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.double">double</a></code></td>
      <td>±5.0&nbsp;×&nbsp;10<sup>−324</sup> to ±1.7&nbsp;×&nbsp;10<sup>308</sup></td>
      <td>~15–17 digits</td>
      <td><code>"double"</code></td>
    </tr>
    <tr valign="top">
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.decimal">decimal</a></code></td>
      <td>±1.0&nbsp;×&nbsp;10<sup>−28</sup> to ±7.9228&nbsp;×&nbsp;10<sup>28</sup></td>
      <td>28–29 significant digits</td>
      <td>
        <code>{<br />&nbsp;&nbsp;"type": "bytes",<br />&nbsp;&nbsp;"logicalType": "decimal",<br />&nbsp;&nbsp;"precision": 29,<br />&nbsp;&nbsp;"scale": 14<br/>}</code>
      </td>
    </tr>
  </tbody>
</table>

Generally speaking, it’s a good idea to fit the precision and scale of a decimal schema to a specific use case. For example, air temperature measurements in ℉ might have a precision of 4 and a scale of 1. (29 and 14, the schema builder defaults, were selected to fit any .NET decimal.) Decimal values are resized to fit the scale specified by the schema—when serializing, digits may be truncated; when deserializing, zeros may be added.

### Caveats

Because the serializer and deserializer rely on predefined conversions, the [remarks from the C# numeric conversions table](https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/explicit-numeric-conversions-table#remarks) are relevant. Notably:

*   Conversions may cause a loss of precision. For instance, if a `"double"` value is deserialized into a <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.single">float</a></code>, the value will be rounded to the nearest <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.single">float</a></code> value:

    ```csharp
    var schema = new DoubleSchema();
    var serializer = new BinarySerializerBuilder().BuildSerializer<double>(schema);
    var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<float>(schema);

    var e = Math.E; // 2.71828182845905
    var bytes = serializer.Serialize(e);

    deserializer.Deserialize(bytes); // 2.718282
    ```

    See the [.NET type conversion tables](https://docs.microsoft.com/en-us/dotnet/standard/base-types/conversion-tables) for a complete list of conversions.

*   <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.overflowexception">OverflowException</a></code> is thrown when a conversion fails during serialization or deserialization.

    When a value is out of the range of a numeric type:

    ```csharp
    var schema = new IntSchema();
    var serializer = new BinarySerializerBuilder().BuildSerializer<int>(schema);
    var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<short>(schema);

    var bytes = serializer.Serialize(int.MaxValue);

    deserializer.Deserialize(bytes); // throws OverflowException
    ```

    When special floating-point values are deserialized to <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.decimal">decimal</a></code>:

    ```csharp
    var schema = new FloatSchema();
    var serializer = new BinarySerializerBuilder().BuildSerializer<float>(schema);
    var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<decimal>(schema);

    var bytes = serializer.Serialize(float.NaN);

    deserializer.Deserialize(bytes); // throws OverflowException
    ```

    Finally, when a serialized integer is too large to deserialize:

    ```csharp
    var schema = new LongSchema();
    var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<long>(schema);

    var bytes = new byte[]
    {
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01
    };

    deserializer.Deserialize(bytes); // throws OverflowException
    ```

## Records

Chr.Avro maps .NET classes and structs to Avro’s `"record"` type by attempting to find a matching constructor. The rules:

*   Parameter names don't need to match the schema exactly—all non-alphanumeric characters are stripped and comparisons are case-insensitive. So, for example, a record field named `addressLine1` will match parameters named `AddressLine1`, `AddressLine_1`, `ADDRESS_LINE_1`, etc.
*   Parameters must have exactly 1 match for each record field.
*   There may be additional optional parameters.

If no matching constructors are found then it will attempt to match each record field to a field or property on the type. The rules:

*   Type member names don’t need to match the schema exactly—all non-alphanumeric characters are stripped and comparisons are case-insensitive. So, for example, a record field named `addressLine1` will match type members named `AddressLine1`, `AddressLine_1`, `ADDRESS_LINE_1`, etc.
*   When the serializer builder and deserializer builder find multiple matching type members, `UnsupportedTypeException` is thrown.
*   When the serializer builder can’t find a matching type member but a default is specified by the schema, the default value will be serialized.
*   When the serializer builder can’t find a matching type member and no default is specified by the schema, `UnsupportedTypeException` is thrown. When the deserializer can’t find a matching type member, the field is ignored.
*   The deserializer builder throws `UnsupportedTypeException` if a type doesn’t have a parameterless public constructor.

By default, Chr.Avro also honors data contract attributes if a <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.runtime.serialization.datacontractattribute">DataContractAttribute</a></code> is present on the type. In that case, two additional rules apply:

*   All type members without a <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.runtime.serialization.datamemberattribute">DataMemberAttribute</a></code> are ignored.
*   If <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.runtime.serialization.datamemberattribute.name">Name</a></code> is set, the custom name must match the record field name exactly. If it’s not set, the type member name will be compared inexactly as described above.

To change or extend this behavior, implement `ITypeResolver` or extend one of the existing resolvers (`ReflectionResolver` and `DataContractResolver`).

## Strings

In addition to <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.string">string</a></code>, Chr.Avro supports mapping the following types to `"string"`:

<table>
  <thead>
    <tr valign='top'>
      <th>.NET&nbsp;type</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr valign='top'>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetime">DateTime</a></code></td>
      <td rowSpan='3'>
        Values are expressed as strings according to <a href="https://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a>. See the <a href="#dates-and-times">dates and times section</a> for details.
      </td>
    </tr>
    <tr valign='top'>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.datetimeoffset">DateTimeOffset</a></code></td>
    </tr>
    <tr valign='top'>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.timespan">TimeSpan</a></code></td>
    </tr>
    <tr valign='top'>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid">Guid</a></code></td>
      <td>
        The <a href="https://docs.microsoft.com/en-us/dotnet/api/system.guid.tostring"><code>Guid.ToString</code> method</a> is used for serialization, and the <a href="https://learn.microsoft.com/en-us/dotnet/api/system.guid.-ctor"><code>Guid</code> constructor</a> is used for deserialization. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code> is thrown when a string cannot be parsed.
      </td>
    </tr>
    <tr valign='top'>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.uri">Uri</a></code></td>
      <td>
        The <a href="https://docs.microsoft.com/en-us/dotnet/api/system.uri.tostring"><code>Uri.ToString</code> method</a> is used for serialization, and the <a href="https://learn.microsoft.com/en-us/dotnet/api/system.uri.-ctor"><code>Uri</code> constructor</a> is used for deserialization. <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.formatexception">FormatException</a></code> is thrown when a string cannot be parsed.
      </td>
    </tr>
  </tbody>
</table>

## Unions

Chr.Avro maps Avro unions to .NET types according to these rules:

*   Unions must contain more than one schema. Avro doesn’t explicitly disallow empty unions, but they can’t be serialized or deserialized.
*   When mapping a union schema to a type for serialization, the type must be able to be mapped to one of the non-`"null"` schemas in the union (if there are any).
*   When mapping a union schema to a type for deserialization, the type must be able to be mapped to all of the schemas in the union.

So, for example:

<table>
  <thead>
    <tr valign='top'>
      <th>Schema</th>
      <th>.NET&nbsp;type</th>
      <th>Serializable</th>
      <th>Deserializable</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr valign='top'>
      <td><code>[]</code></td>
      <td></td>
      <td align='center'><span role="img" aria-label="not serializable">🚫</span></td>
      <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
      <td>
        Empty unions are not supported.
      </td>
    </tr>
    <tr valign='top'>
      <td><code>["int"]</code></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code></td>
      <td align='center'><span role="img" aria-label="serializable">✅</span></td>
      <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
      <td>
        <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code> could be serialized and deserialized as <code>"int"</code>.
      </td>
    </tr>
    <tr valign='top'>
      <td><code>["null"]</code></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code></td>
      <td align='center'><span role="img" aria-label="serializable">✅</span></td>
      <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
      <td>
        <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code> could be serialized as <code>"null"</code>, but it couldn’t be deserialized as <code>"null"</code>.
      </td>
    </tr>
    <tr valign='top'>
      <td><code>["int", "string"]</code></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code></td>
      <td align='center'><span role="img" aria-label="serializable">✅</span></td>
      <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
      <td>
        <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code> could be serialized as <code>"int"</code>, but it couldn’t be deserialized as <code>"string"</code>.
      </td>
    </tr>
    <tr valign='top'>
      <td><code>["null", "int"]</code></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code></td>
      <td align='center'><span role="img" aria-label="serializable">✅</span></td>
      <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
      <td>
        <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a></code> could be serialized as <code>"int"</code>, but it couldn’t be deserialized as <code>"null"</code>.
      </td>
    </tr>
    <tr valign='top'>
      <td><code>["null", "int"]</code></td>
      <td><code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a><a href="https://docs.microsoft.com/en-us/dotnet/api/system.nullable-1">?</a></code></td>
      <td align='center'><span role="img" aria-label="serializable">✅</span></td>
      <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
      <td>
        <code><a href="https://docs.microsoft.com/en-us/dotnet/api/system.int32">int</a><a href="https://docs.microsoft.com/en-us/dotnet/api/system.nullable-1">?</a></code> could be serialized and deserialized as either <code>"null"</code> or <code>"int"</code>.
      </td>
    </tr>
  </tbody>
</table>
