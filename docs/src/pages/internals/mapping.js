import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import DotnetReference from '../../components/references/dotnet'
import ExternalLink from '../../components/site/external-link'

const title = 'Types and conversions'

export default function MappingPage () {
  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>

      <h1>{title}</h1>
      <p>Chr.Avro supports mapping .NET’s <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/built-in-types-table'>built-in types</ExternalLink>, as well as commonly used types like <DotnetReference id='T:System.DateTime' /> and <DotnetReference id='T:System.Uri' />, to Avro schemas. This document is a comprehensive explanation of how that mapping works.</p>

      <p>The <DotnetReference id='T:Chr.Avro.Serialization.BinarySerializerBuilder'>serializer builder</DotnetReference> and <DotnetReference id='T:Chr.Avro.Serialization.BinaryDeserializerBuilder'>deserializer builder</DotnetReference> generally throw <DotnetReference id='T:System.AggregateException' /> when a type can’t be mapped to a schema. Other exceptions, usually <DotnetReference id='T:System.OverflowException' /> and <DotnetReference id='T:System.FormatException' />, are thrown when errors occur during serialization or deserialization.</p>

      <h2 id='arrays'>Arrays</h2>
      <p>Avro specifies an <Highlight inline language='avro'>"array"</Highlight> type for variable-length lists of items. Chr.Avro can map a .NET type to <Highlight inline language='avro'>"array"</Highlight> if any of the following is true:</p>
      <ol>
        <li>
          <p>The type is a one-dimensional or jagged array. Multi-dimensional arrays are currently not supported because they can’t be deserialized reliably.</p>
        </li>
        <li>
          <p>The type is an <DotnetReference id='T:System.ArraySegment`1' /> type, a <DotnetReference id='T:System.Collections.ObjectModel.Collection`1' /> type, or a generic collection type from <DotnetReference id='N:System.Collections.Generic' /> or <DotnetReference id='N:System.Collections.Immutable' />.</p>
        </li>
        <li>
          <p>The type implements <DotnetReference id='T:System.Collections.Generic.IEnumerable`1' /> (for serialization) and has a constructor with a single <DotnetReference id='T:System.Collections.Generic.IEnumerable`1' /> parameter (for deserialization).</p>
        </li>
      </ol>
      <p>Some examples:</p>
      <table>
        <thead>
          <tr valign='top'>
            <th>.NET&nbsp;type</th>
            <th>Serializable</th>
            <th>Deserializable</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Int32[]' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Int32[]' /> is a one-dimensional array type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Int32[,,]' /></td>
            <td align='center'><span role="img" aria-label="not serializable">🚫</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td><DotnetReference id='T:System.Int32[,,]' /> is a multi-dimensional array type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Int32[][]' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Int32[][]' /> is a jagged array type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.IEnumerable{System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.IEnumerable{System.Int32}' /> is a generic collection type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.ISet{System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.ISet{System.Int32}' /> is a generic collection type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.List{System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.List{System.Int32}' /> is a generic collection type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.List{System.Int32[]}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.List{System.Int32[]}' /> is a generic collection type and <DotnetReference id='T:System.Int32[]' /> is an array type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Immutable.ImmutableQueue{System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Immutable.ImmutableQueue{System.Int32}' /> is a generic collection type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Array' /></td>
            <td align='center'><span role="img" aria-label="not serializable">🚫</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td><DotnetReference id='T:System.Array' /> isn’t a generic type, so Chr.Avro can’t determine how to handle its items.</td>
          </tr>
        </tbody>
      </table>

      <h2 id='booleans'>Booleans</h2>
      <p>Chr.Avro maps Avro’s <Highlight inline language='avro'>"boolean"</Highlight> primitive type to the .NET <DotnetReference id='T:System.Boolean' /> type. No implicit conversions exist between <DotnetReference id='T:System.Boolean' /> and other types (see the <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/bool#conversions'>.NET docs</ExternalLink>), so no other mappings are supported.</p>

      <h2 id='byte-arrays'>Byte arrays</h2>
      <p>In addition to <DotnetReference id='T:System.Byte[]' />, Chr.Avro supports mapping the following types to <Highlight inline language='avro'>"bytes"</Highlight> and <Highlight inline language='avro'>"fixed"</Highlight>:</p>
      <table>
        <thead>
          <tr valign='top'>
            <th>.NET&nbsp;type</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Guid' /></td>
            <td>
              The <DotnetReference id='M:System.Guid.ToByteArray'><Highlight inline language='csharp'>Guid.ToByteArray</Highlight> method</DotnetReference> is used for serialization, and the <DotnetReference id='M:System.Guid.#ctor(System.Byte[])'><Highlight inline language='csharp'>Guid</Highlight> constructor</DotnetReference> is used for deserialization. <DotnetReference id='T:System.ArgumentException' /> is thrown when the length is not 16.
            </td>
          </tr>
        </tbody>
      </table>

      <h2 id='dates-and-times'>Dates and times</h2>
      <p>The Avro spec defines six logical types for temporal data:</p>
      <ul>
        <li>calendar dates with no time or time zone (<Highlight inline language='avro'>"date"</Highlight>)</li>
        <li>duration comprised of months, days, and milliseconds (<Highlight inline language='avro'>"duration"</Highlight>)</li>
        <li>times of day with no date or time zone (<Highlight inline language='avro'>"time-millis"</Highlight> and <Highlight inline language='avro'>"time-micros"</Highlight>)</li>
        <li>instants in time (<Highlight inline language='avro'>"timestamp-millis"</Highlight> and <Highlight inline language='avro'>"timestamp-micros"</Highlight>)</li>
      </ul>
      <p>In addition to the conversions described later in this section, these logical types can be treated as their underlying primitive types:</p>
      <Highlight language='csharp'>{`var schema = new MillisecondTimestampSchema();
  var serializer = new BinarySerializerBuilder().BuildSerializer<DateTime>();
  var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<long>();

  var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
  deserializer.Deserialize(serializer.Serialize(epoch)); // 0L`}</Highlight>
      <p>.NET doesn’t include any types that match calendar date or time of day semantics, so no special mappings are provided for <Highlight inline language='avro'>"date"</Highlight>, <Highlight inline language='avro'>"time-millis"</Highlight>, and <Highlight inline language='avro'>"time-micros"</Highlight>.</p>
      <h3>Durations</h3>
      <p>.NET’s <DotnetReference id='T:System.TimeSpan' /> struct is commonly used to represent durations. Under the hood, a <DotnetReference id='T:System.TimeSpan' /> stores its value as a number of ticks (1 tick = 100 ns = 0.0001 ms). However, <Highlight inline language='avro'>"duration"</Highlight> values can’t be reliably converted to a number of ticks; there isn’t a consistent number of milliseconds in a day or a consistent number of days in a month. Also, all three components are represented as unsigned integers, so negative durations cannot be expressed.</p>
      <p>In light of those incompatibilities, Chr.Avro prefers to map <DotnetReference id='T:System.TimeSpan' />s to <ExternalLink to='https://en.wikipedia.org/wiki/ISO_8601#Durations'>ISO 8601 strings</ExternalLink>, avoiding <Highlight inline language='avro'>"duration"</Highlight> when building schemas. <DotnetReference id='M:System.Xml.XmlConvert.ToString(System.TimeSpan)'><Highlight inline language='csharp'>XmlConvert.ToString</Highlight></DotnetReference> and <DotnetReference id='M:System.Xml.XmlConvert.ToTimeSpan(System.String)'><Highlight inline language='csharp'>XmlConvert.ToTimeSpan</Highlight></DotnetReference> are used for conversions. <DotnetReference id='T:System.FormatException' /> is thrown when a string cannot be parsed.</p>
      <p>Serializing and deserializing <Highlight inline language='avro'>"duration"</Highlight> values still work, though there are some limitations. .NET assumes a <DotnetReference id='F:System.TimeSpan.TicksPerDay'>consistent number of milliseconds in a day</DotnetReference>, so Chr.Avro supports the day and millisecond components. (This may lead to minor arithmetic inconsistencies with other platforms.) All non-negative <DotnetReference id='T:System.TimeSpan' />s can be serialized without using the months component. <DotnetReference id='T:System.OverflowException' /> is thrown when serializing a negative <DotnetReference id='T:System.TimeSpan' /> and when deserializing a value with a non-zero months component.</p>
      <h3>Timestamps</h3>
      <p>Both <DotnetReference id='T:System.DateTime' /> and <DotnetReference id='T:System.DateTimeOffset' /> can be used to represent timestamps. Chr.Avro prefers to map those types to <ExternalLink to='https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations'>ISO 8601 strings</ExternalLink>, avoiding <Highlight inline language='avro'>"timestamp-millis"</Highlight> and <Highlight inline language='avro'>"timestamp-micros"</Highlight> when building schemas. This behavior is consistent with how durations are handled, and it also means that <DotnetReference id='T:System.DateTime' /> kind and timezone are retained—the <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/standard/base-types/standard-date-and-time-format-strings#Roundtrip'>round-trip (“O”, “o”) format specifier</ExternalLink> is used for serialization. <DotnetReference id='T:System.FormatException' /> is thrown when a string cannot be parsed.</p>
      <p>Serializing and deserializing <Highlight inline language='avro'>"timestamp-millis"</Highlight> and <Highlight inline language='avro'>"timestamp-micros"</Highlight> values are supported as well, with a few caveats:</p>
      <ul>
        <li>All <DotnetReference id='T:System.DateTime' />s are converted to UTC. Don’t use <DotnetReference id='T:System.DateTime' />s with kind unspecified.</li>
        <li>.NET date types are tick-precision, so serializing to <Highlight inline language='avro'>"timestamp-millis"</Highlight> or deserializing from <Highlight inline language='avro'>"timestamp-micros"</Highlight> may result in a loss of precision.</li>
      </ul>

      <h2 id='enums'>Enums</h2>
      <p>Chr.Avro maps .NET enumerations to Avro’s <Highlight inline language='avro'>"enum"</Highlight> type by matching each symbol on the schema to an enumerator on the enumeration according to these rules:</p>
      <ul>
        <li>
          <p>Enumerator names don’t need to be an exact match—all non-alphanumeric characters are stripped and comparisons are case-insensitive. For example, a <code>PRIMARY_RESIDENCE</code> symbol will match enumerators named <code>PrimaryResidence</code>, <code>primaryResidence</code>, etc.</p>
        </li>
        <li>
          <p>When the serializer builder and deserializer builder find multiple matching enumerators, <DotnetReference id='T:System.AggregateException' /> is thrown.</p>
        </li>
        <li>
          <p>When the deserializer builder can’t find a matching enumerator but a default is specified by the schema, the deserializer builder will attempt to map the default instead.</p>
        </li>
        <li>
          <p>When the deserializer builder can’t find a matching enumerator and no default is specified by the schema, <DotnetReference id='T:System.AggregateException' /> is thrown.</p>
        </li>
      </ul>
      <p>By default, Chr.Avro also honors data contract attributes if a <DotnetReference id='T:System.Runtime.Serialization.DataContractAttribute' /> is present on the enumeration. In that case, if <DotnetReference id='P:System.Runtime.Serialization.EnumMemberAttribute.Value' /> is set on an enumerator, the custom value must match the symbol exactly. If it’s not set, the enumerator name will be compared inexactly as described above.</p>
      <p>To change or extend this behavior, implement <DotnetReference id='T:Chr.Avro.Resolution.ITypeResolver' /> or extend one of the existing resolvers (<DotnetReference id='T:Chr.Avro.Resolution.ReflectionResolver' /> and <DotnetReference id='T:Chr.Avro.Resolution.DataContractResolver' />).</p>
      <p>Because <Highlight inline language='avro'>"enum"</Highlight> symbols are represented as strings, Chr.Avro also supports mapping enum schemas to <DotnetReference id='T:System.String' />. On serialization, if the name of the enumerator is not a symbol in the schema, <DotnetReference id='T:System.ArgumentException' /> will be thrown.</p>

      <h2 id='maps'>Maps</h2>
      <p>Avro’s <Highlight inline language='avro'>"map"</Highlight> type represents a map of keys (assumed to be strings) to values. Chr.Avro can map a .NET type to <Highlight inline language='avro'>"map"</Highlight> if any of the following is true:</p>
      <ol>
        <li>
          <p>The type is a generic dictionary type from <DotnetReference id='N:System.Collections.Generic' /> or <DotnetReference id='N:System.Collections.Immutable' />.</p>
        </li>
        <li>
          <p>The type implements <DotnetReference id='T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}' /> (for serialization) and has a constructor with a single <DotnetReference id='T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}' /> parameter (for deserialization).</p>
        </li>
      </ol>
      <p>Additionally, because Avro map keys are assumed to be strings, serializers and deserializers are built for key types by mapping to <Highlight inline language='avro'>"string"</Highlight> implicitly.</p>
      <p>Some examples of this behavior:</p>
      <table>
        <thead>
          <tr valign='top'>
            <th>.NET&nbsp;type</th>
            <th>Serializable</th>
            <th>Deserializable</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.IDictionary{System.String,System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.IDictionary{System.String,System.Int32}' /> is a generic dictionary type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.Dictionary{System.String,System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.Dictionary{System.String,System.Int32}' /> is a generic dictionary type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.IDictionary{System.Guid,System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.IDictionary{System.Guid,System.Int32}' /> is a generic dictionary type, and <DotnetReference id='T:System.Guid' /> can be mapped to <Highlight inline language='avro'>"string"</Highlight>.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.IDictionary{System.Byte[],System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="not serializable">🚫</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.IDictionary{System.Byte[],System.Int32}' /> is a generic dictionary type, but <DotnetReference id='T:System.Byte[]' /> cannot be mapped to <Highlight inline language='avro'>"string"</Highlight>.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair{System.String,System.Int32}}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair{System.String,System.Int32}}' /> is recognized as a generic dictionary type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.ICollection{System.Collections.Generic.KeyValuePair{System.String,System.Int32}}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.ICollection{System.Collections.Generic.KeyValuePair{System.String,System.Int32}}' /> is recognized as a generic dictionary type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Immutable.ImmutableSortedDictionary{System.String,System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td><DotnetReference id='T:System.Collections.Immutable.ImmutableSortedDictionary{System.String,System.Int32}' /> is a generic dictionary type.</td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Collections.Generic.IEnumerable{System.ValueTuple{System.String,System.Int32}}' /></td>
            <td align='center'><span role="img" aria-label="not serializable">🚫</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td><DotnetReference id='T:System.Collections.Generic.IEnumerable{System.ValueTuple{System.String,System.Int32}}' /> is not recognized as a generic dictionary type.</td>
          </tr>
        </tbody>
      </table>

      <h2 id='numbers'>Numbers</h2>
      <p>The Avro spec defines <ExternalLink to='https://avro.apache.org/docs/current/spec.html#schema_primitive'>four primitive numeric types</ExternalLink>:</p>
      <ul>
        <li>32-bit signed integers (<Highlight inline language='avro'>"int"</Highlight>)</li>
        <li>64-bit signed integers (<Highlight inline language='avro'>"long"</Highlight>)</li>
        <li>single-precision (32-bit) floating-point numbers (<Highlight inline language='avro'>"float"</Highlight>)</li>
        <li>double-precision (64-bit) floating-point numbers (<Highlight inline language='avro'>"double"</Highlight>)</li>
      </ul>
      <p>It also defines a logical <ExternalLink to='https://avro.apache.org/docs/current/spec.html#Decimal'><Highlight inline language='avro'>"decimal"</Highlight></ExternalLink> type that supports arbitrary-precision decimal numbers.</p>
      <h3>Integral types</h3>
      <p>When generating a schema, Chr.Avro maps integral types less than or equal to 32 bits to <Highlight inline language='avro'>"int"</Highlight> and integral types greater than 32 bits to <Highlight inline language='avro'>"long"</Highlight>:</p>
      <table>
        <thead>
          <tr valign='top'>
            <th>.NET&nbsp;type</th>
            <th>Range</th>
            <th align='right'>Bits</th>
            <th>Generated&nbsp;schema</th>
          </tr>
        </thead>
        <tbody>
          <tr valign='top'>
            <td><DotnetReference id='T:System.SByte' /></td>
            <td>−128 to 127</td>
            <td align='right'>8</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Byte' /></td>
            <td>0 to 255</td>
            <td align='right'>8</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Int16' /></td>
            <td>−32,768 to 32,767</td>
            <td align='right'>16</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.UInt16' /></td>
            <td>0 to 65,535</td>
            <td align='right'>16</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Char' /></td>
            <td>0 (U+0000) to 65,535 (U+ffff)</td>
            <td align='right'>16</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Int32' /></td>
            <td>−2,147,483,648 to 2,147,483,647</td>
            <td align='right'>32</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.UInt32' /></td>
            <td>0 to 4,294,967,295</td>
            <td align='right'>32</td>
            <td><Highlight language='avro'>"int"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Int64' /></td>
            <td>−9,223,372,036,854,775,808 to 9,223,372,036,854,775,807</td>
            <td align='right'>64</td>
            <td><Highlight language='avro'>"long"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.UInt64' /></td>
            <td>0 to 18,446,744,073,709,551,615</td>
            <td align='right'>64</td>
            <td><Highlight language='avro'>"long"</Highlight></td>
          </tr>
        </tbody>
      </table>
      <p>Whether a schema is <Highlight inline language='avro'>"int"</Highlight> or <Highlight inline language='avro'>"long"</Highlight> has no impact on serialization. Integers are <ExternalLink to='https://avro.apache.org/docs/current/spec.html#binary_encoding'>zig-zag encoded</ExternalLink>, so they take up as much space as they need. For that reason, Chr.Avro imposes no constraints on which numeric types can be serialized or deserialized to <Highlight inline language='avro'>"int"</Highlight> or <Highlight inline language='avro'>"long"</Highlight>—if a conversion exists, the binary serializer and deserializer will use it.</p>
      <p>Because enum types are able to be implicitly converted to and from integral types, Chr.Avro can map any enum type to <Highlight inline language='avro'>"int"</Highlight> or <Highlight inline language='avro'>"long"</Highlight> as well.</p>
      <h3>Non-integral types</h3>
      <p>On the non-integral side, .NET types are mapped to their respective Avro types:</p>
      <table>
        <thead>
          <tr valign='top'>
            <th>.NET&nbsp;type</th>
            <th>Approximate range</th>
            <th>Precision</th>
            <th>Generated schema</th>
          </tr>
        </thead>
        <tbody>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Single' /></td>
            <td>±1.5&nbsp;×&nbsp;10<sup>−45</sup> to ±3.4&nbsp;×&nbsp;10<sup>38</sup></td>
            <td>~6–9 digits</td>
            <td><Highlight language='avro'>"float"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Double' /></td>
            <td>±5.0&nbsp;×&nbsp;10<sup>−324</sup> to ±1.7&nbsp;×&nbsp;10<sup>308</sup></td>
            <td>~15–17 digits</td>
            <td><Highlight language='avro'>"double"</Highlight></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Decimal' /></td>
            <td>±1.0&nbsp;×&nbsp;10<sup>−28</sup> to ±7.9228&nbsp;×&nbsp;10<sup>28</sup></td>
            <td>28–29 significant digits</td>
            <td>
              <Highlight language='avro'>{`{
    "type": "bytes",
    "logicalType": "decimal",
    "precision": 29,
    "scale": 14
  }`}</Highlight>
            </td>
          </tr>
        </tbody>
      </table>
      <p>Generally speaking, it’s a good idea to fit the precision and scale of a decimal schema to a specific use case. For example, air temperature measurements in ℉ might have a precision of 4 and a scale of 1. (29 and 14, the schema builder defaults, were selected to fit any .NET decimal.) Decimal values are resized to fit the scale specified by the schema—when serializing, digits may be truncated; when deserializing, zeros may be added.</p>
      <h3>Caveats</h3>
      <p>Because the serializer and deserializer rely on predefined conversions, the <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/csharp/language-reference/keywords/explicit-numeric-conversions-table#remarks'>remarks from the C# numeric conversions table</ExternalLink> are relevant. Notably:</p>
      <ul>
        <li>
          <p>Conversions may cause a loss of precision. For instance, if a <Highlight inline language='avro'>"double"</Highlight> value is deserialized into a <DotnetReference id='T:System.Single' />, the value will be rounded to the nearest <DotnetReference id='T:System.Single' /> value:</p>
          <Highlight language='csharp'>{`var schema = new DoubleSchema();
  var serializer = new BinarySerializerBuilder().BuildSerializer<double>(schema);
  var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<float>(schema);

  var e = Math.E; // 2.71828182845905
  var bytes = serializer.Serialize(e);

  deserializer.Deserialize(bytes); // 2.718282`}</Highlight>
          <p>See the <ExternalLink to='https://docs.microsoft.com/en-us/dotnet/standard/base-types/conversion-tables'>.NET type conversion tables</ExternalLink> for a complete list of conversions.</p>
        </li>
        <li>
          <p><DotnetReference id='T:System.OverflowException' /> is thrown when a conversion fails during serialization or deserialization.</p>
          <p>When a value is out of the range of a numeric type:</p>
          <Highlight language='csharp'>{`var schema = new IntSchema();
  var serializer = new BinarySerializerBuilder().BuildSerializer<int>(schema);
  var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<short>(schema);

  var bytes = serializer.Serialize(int.MaxValue);

  deserializer.Deserialize(bytes); // throws OverflowException`}</Highlight>
          <p>When special floating-point values are deserialized to <DotnetReference id='T:System.Decimal' />:</p>
          <Highlight language='csharp'>{`var schema = new FloatSchema();
  var serializer = new BinarySerializerBuilder().BuildSerializer<float>(schema);
  var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<decimal>(schema);

  var bytes = serializer.Serialize(float.NaN);

  deserializer.Deserialize(bytes); // throws OverflowException`}</Highlight>
          <p>Finally, when a serialized integer is too large to deserialize:</p>
          <Highlight language='csharp'>{`var schema = new LongSchema();
  var deserializer = new BinaryDeserializerBuilder().BuildDeserializer<long>(schema);

  var bytes = new byte[]
  {
      0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01
  };

  deserializer.Deserialize(bytes); // throws OverflowException`}</Highlight>
        </li>
      </ul>

      <h2 id='records'>Records</h2>
      <p>Chr.Avro maps .NET classes and structs to Avro’s <Highlight inline language='avro'>"record"</Highlight> type by attempting to find a matching constructor. The rules:</p>
      <ul>
        <li>
          <p>Parameter names don't need to match the schema exactly—all non-alphanumeric characters are stripped and comparisons are case-insensitive. So, for example, a record field named <code>addressLine1</code> will match parameters named <code>AddressLine1</code>, <code>AddressLine_1</code>, <code>ADDRESS_LINE_1</code>, etc.</p>
        </li>
        <li>
          <p>Parameters must have exactly 1 match for each record field.</p>
        </li>
        <li>
          <p>There may be additional optional parameters.</p>
        </li>
      </ul>
      <p>If no matching constructors are found then it will attempt to match each record field to a field or property on the type. The rules:</p>
      <ul>
        <li>
          <p>Type member names don’t need to match the schema exactly—all non-alphanumeric characters are stripped and comparisons are case-insensitive. So, for example, a record field named <code>addressLine1</code> will match type members named <code>AddressLine1</code>, <code>AddressLine_1</code>, <code>ADDRESS_LINE_1</code>, etc.</p>
        </li>
        <li>
          <p>When the serializer builder and deserializer builder find multiple matching type members, <DotnetReference id='T:System.AggregateException' /> is thrown.</p>
        </li>
        <li>
          <p>When the serializer builder can’t find a matching type member but a default is specified by the schema, the default value will be serialized.</p>
        </li>
        <li>
          <p>When the serializer builder can’t find a matching type member and no default is specified by the schema, <DotnetReference id='T:System.AggregateException' /> is thrown. When the deserializer can’t find a matching type member, the field is ignored.</p>
        </li>
        <li>
          <p>The deserializer builder throws <DotnetReference id='T:System.AggregateException' /> if a type doesn’t have a parameterless public constructor.</p>
        </li>
      </ul>
      <p>By default, Chr.Avro also honors data contract attributes if a <DotnetReference id='T:System.Runtime.Serialization.DataContractAttribute' /> is present on the type. In that case, two additional rules apply:</p>
      <ul>
        <li>
          <p>All type members without a <DotnetReference id='T:System.Runtime.Serialization.DataMemberAttribute' /> are ignored.</p>
        </li>
        <li>
          <p>If <DotnetReference id='P:System.Runtime.Serialization.DataContractAttribute.Name' /> is set, the custom name must match the record field name exactly. If it’s not set, the type member name will be compared inexactly as described above.</p>
        </li>
      </ul>
      <p>To change or extend this behavior, implement <DotnetReference id='T:Chr.Avro.Resolution.ITypeResolver' /> or extend one of the existing resolvers (<DotnetReference id='T:Chr.Avro.Resolution.ReflectionResolver' /> and <DotnetReference id='T:Chr.Avro.Resolution.DataContractResolver' />).</p>

      <h2 id='strings'>Strings</h2>
      <p>In addition to <DotnetReference id='T:System.String' />, Chr.Avro supports mapping the following types to <Highlight inline language='avro'>"string"</Highlight>:</p>
      <table>
        <thead>
          <tr valign='top'>
            <th>.NET&nbsp;type</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr valign='top'>
            <td><DotnetReference id='T:System.DateTime' /></td>
            <td rowSpan='3'>
              Values are expressed as strings according to <ExternalLink to='https://en.wikipedia.org/wiki/ISO_8601'>ISO 8601</ExternalLink>. See the <a href='#dates-and-times'>dates and times section</a> for details.
            </td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.DateTimeOffset' /></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.TimeSpan' /></td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Guid' /></td>
            <td>
              The <DotnetReference id='M:System.Guid.ToString'><Highlight inline language='csharp'>Guid.ToString</Highlight> method</DotnetReference> is used for serialization, and the <DotnetReference id='M:System.Guid.#ctor(System.String)'><Highlight inline language='csharp'>Guid</Highlight> constructor</DotnetReference> is used for deserialization. <DotnetReference id='T:System.FormatException' /> is thrown when a string cannot be parsed.
            </td>
          </tr>
          <tr valign='top'>
            <td><DotnetReference id='T:System.Uri' /></td>
            <td>
              The <DotnetReference id='M:System.Uri.ToString'><Highlight inline language='csharp'>Uri.ToString</Highlight> method</DotnetReference> is used for serialization, and the <DotnetReference id='M:System.Uri.#ctor(System.String)'><Highlight inline language='csharp'>Uri</Highlight> constructor</DotnetReference> is used for deserialization. <DotnetReference id='T:System.FormatException' /> is thrown when a string cannot be parsed.
            </td>
          </tr>
        </tbody>
      </table>

      <h2 id='unions'>Unions</h2>
      <p>Chr.Avro maps Avro unions to .NET types according to these rules:</p>
      <ul>
        <li>
          <p>Unions must contain more than one schema. Avro doesn’t explicitly disallow empty unions, but they can’t be serialized or deserialized.</p>
        </li>
        <li>
          <p>When mapping a union schema to a type for serialization, the type must be able to be mapped to one of the non-<Highlight inline language='avro'>"null"</Highlight> schemas in the union (if there are any).</p>
        </li>
        <li>
          <p>When mapping a union schema to a type for deserialization, the type must be able to be mapped to all of the schemas in the union.</p>
        </li>
      </ul>
      <p>So, for example:</p>
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
            <td><Highlight language='avro'>[]</Highlight></td>
            <td></td>
            <td align='center'><span role="img" aria-label="not serializable">🚫</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td>
              Empty unions are not supported.
            </td>
          </tr>
          <tr valign='top'>
            <td><Highlight language='avro'>["int"]</Highlight></td>
            <td><DotnetReference id='T:System.Int32' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td>
              <DotnetReference id='T:System.Int32' /> could be serialized and deserialized as <Highlight inline language='avro'>"int"</Highlight>.
            </td>
          </tr>
          <tr valign='top'>
            <td><Highlight language='avro'>["null"]</Highlight></td>
            <td><DotnetReference id='T:System.Int32' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td>
              <DotnetReference id='T:System.Int32' /> could be serialized as <Highlight inline language='avro'>"null"</Highlight>, but it couldn’t be deserialized as <Highlight inline language='avro'>"null"</Highlight>.
            </td>
          </tr>
          <tr valign='top'>
            <td><Highlight language='avro'>["int","string"]</Highlight></td>
            <td><DotnetReference id='T:System.Int32' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td>
              <DotnetReference id='T:System.Int32' /> could be serialized as <Highlight inline language='avro'>"int"</Highlight>, but it couldn’t be deserialized as <Highlight inline language='avro'>"string"</Highlight>.
            </td>
          </tr>
          <tr valign='top'>
            <td><Highlight language='avro'>["null","int"]</Highlight></td>
            <td><DotnetReference id='T:System.Int32' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="not deserializable">🚫</span></td>
            <td>
              <DotnetReference id='T:System.Int32' /> could be serialized as <Highlight inline language='avro'>"int"</Highlight>, but it couldn’t be deserialized as <Highlight inline language='avro'>"null"</Highlight>.
            </td>
          </tr>
          <tr valign='top'>
            <td><Highlight language='avro'>["null","int"]</Highlight></td>
            <td><DotnetReference id='T:System.Nullable{System.Int32}' /></td>
            <td align='center'><span role="img" aria-label="serializable">✅</span></td>
            <td align='center'><span role="img" aria-label="deserializable">✅</span></td>
            <td>
              <DotnetReference id='T:System.Nullable{System.Int32}' /> could be serialized and deserialized as either <Highlight inline language='avro'>"null"</Highlight> or <Highlight inline language='avro'>"int"</Highlight>.
            </td>
          </tr>
        </tbody>
      </table>
    </>
  )
}
