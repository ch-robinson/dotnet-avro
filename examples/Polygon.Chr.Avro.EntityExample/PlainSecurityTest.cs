using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.Json;
using Chr.Avro;
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using Polygon.Chr.Avro.EntityUnion;
using Polygon.Data;
using Polygon.Data.ReferenceMaster;
using Polygon.Extensions;

namespace Polygon.Chr.Avro.EntityExample
{
    public class PlainSecurityTest
    {

        public static bool IgnoreField(Type memberType, MemberInfo member)
        {
            if (memberType.GetInterfaces().Any(z => z.Name.StartsWith("IPlainEntity")))
                return true;

            if (memberType.IsGenericType && memberType.GetGenericTypeDefinition().Name == "Option`1")
            {
                var ot = memberType.GetGenericArguments()[0];
                if (ot != null && ot.GetInterfaces().Any(z => z.Name.StartsWith("IPlainEntity")))
                    return true;
            }


            if (GetFieldName(member) == "Children")
                return true;
            if (GetFieldName(member) == "Components")
                return true;
            if (GetFieldName(member) == "Dependencies")
                return true;
            if (GetFieldName(member) == "PlainCache")
                return true;
            if (GetFieldName(member) == "Changed")
                return true;




            if (memberType == typeof(IEntity))
                return true;
            ;
            if (memberType == typeof(IEntityEdit))
                return true;
            if (memberType == typeof(IEntitySetRead))
                return true;
            if (memberType == typeof(ICorporateAction))
                return true;


            // need to handle these via a special Case
            if (memberType == typeof(NodaTime.LocalDate) || memberType == typeof(Nullable<NodaTime.LocalDate>))
                return true;
            ;

            if (memberType == typeof(NodaTime.LocalDateTime) || memberType == typeof(Nullable<NodaTime.LocalDateTime>))
                return true;

            NullableEntityId a;
            return false;
        }


        protected static string GetFieldName(MemberInfo member)
        {
            T? GetAttribute<T>(MemberInfo member)
                where T : Attribute
            {
                return member.GetCustomAttributes(typeof(T), true)
                    .OfType<T>()
                    .SingleOrDefault();
            }

            if (member.DeclaringType.HasAttribute<DataContractAttribute>()
                && GetAttribute<DataMemberAttribute>(member) is DataMemberAttribute memberAttribute
                && !string.IsNullOrEmpty(memberAttribute.Name))
            {
                return memberAttribute.Name;
            }
            else
            {
                return member.Name;
            }
        }


        public void Test()
        {

            RecordSchemaBuilderCase.IgnoreField = IgnoreField;

            var unionRegistry = new UnionRegistry();
            unionRegistry.RegisterImplementationsOf(typeof(IPlainEntity), typeof(PlainSecurity).Assembly);
            unionRegistry.RegisterImplementationsOf(typeof(IBondDividendProtection));
            //unionRegistry.RegisterImplementationsOf(typeof(ICorporateAction)); // there is only one thing in this union, so we can't do it



            var caseBuilders = SchemaBuilder.CreateDefaultCaseBuilders(nullableReferenceTypeBehavior: NullableReferenceTypeBehavior.All)
                .Prepend(b =>
                    {
                        var c = new EntityUnionSchemaBuilderCase(b, unionRegistry);
                        return c;
                    }
                )
                .Append(b => new RecordSchemaBuilderCase(BindingFlags.Public | BindingFlags.Instance, NullableReferenceTypeBehavior.All, b))

                ;


            var builder = new SchemaBuilder(caseBuilders: caseBuilders);


            PlainSecurity clone;
            using (var reader = new StreamReader("C:/temp/plainsecurity.obj"))
            {
                BinaryFormatter binaryFormatter = new BinaryFormatter();
                try
                {
                    clone = (PlainSecurity)binaryFormatter.Deserialize(reader.BaseStream);
                }
                catch (SerializationException ex)
                {
                    throw new SerializationException(((object)ex).ToString() + "\n" + ex.Source);
                }
            }

            var a = new SerializedPlainEntity<PlainSecurity>(clone);
            Serialize(builder, unionRegistry, a);
        }

        public class SerializedPlainEntity<T> where T : IPlainEntity
        {
            public PlainMemoizeCache Cache { get; set; }
            public EntityId EntityId { get; set; }



            public SerializedPlainEntity()
            {

            }


            public SerializedPlainEntity(T plain)
            {
                Cache = plain.Components;
                EntityId = new EntityId(typeof(T), plain.Id);
            }
        }




        private static void Serialize<T>(SchemaBuilder builder, UnionRegistry unions, T clone)
        {
            var schema = builder.BuildSchema<T>();
            var deserializerBuilder = new JsonDeserializerBuilder(JsonDeserializerBuilder.CreateDefaultCaseBuilders()
                .Prepend(b => new EntityUnionDeserializerBuilderCase(b, unions)));
            var serializerBuilder = new JsonSerializerBuilder(JsonSerializerBuilder.CreateDefaultCaseBuilders()
                .Prepend(b => new EntityUnionSerializerBuilderCase(b, unions))

            );
            var serialize = serializerBuilder.BuildDelegate<T>(schema);
            //var deserialize = deserializerBuilder.BuildDelegate<T>(schema);

            JsonWriterOptions writerOptions = new() { Indented = true, };

            var writer = new JsonSchemaWriter();
            //((List<IJsonSchemaWriterCase>)writer.Cases).Add(new JsonOptionSchemaWriterCase(writer));
            using (var stream = System.IO.File.Create("C:/temp/schema.json"))
            {
                var json = new Utf8JsonWriter(stream, writerOptions);
                writer.Write(schema, json);
                json.Flush();
            }


            using (var stream = System.IO.File.Create("C:/temp/aa.txt"))
            {
                serialize(clone, new Utf8JsonWriter(stream, writerOptions));
            }

            var deserialize = deserializerBuilder.BuildDelegate<T>(schema);



            var txt = System.IO.File.ReadAllBytes("C:/temp/aa.txt");
            new Utf8JsonReader(txt);
            using (var stream = new StreamReader("C:/temp/aa.txt"))
            {
                var reader = new Utf8JsonReader(txt);
                var des = (SerializedPlainEntity<PlainSecurity>)(object)deserialize(ref reader);
                var sec = (PlainSecurity)des.Cache.PlainCache[new EntityId(typeof(PlainSecurity).FullName, 12866)];
                foreach (var s in des.Cache.PlainCache.Values)
                    s.Components = des.Cache;
                var equity = sec.EmbeddedEquity;
                var s2 = sec.EmbeddedPriceCurrency.EmbeddedSec;
            }
        }
    }

    public class A
    {
        public string Contents { get; set; }
    }



    public class ObservableSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BytesSchema" /> with a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="BytesSchema" />
        /// and associated <see cref="DecimalLogicalType" /> if <paramref name="type" /> is
        /// <see cref="decimal" />; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(IObservable<>)))
            {

                var nullSchema = new NullSchema
                {
                };

                try
                {
                    context.Schemas.Add(type, nullSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(nullSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(DecimalSchemaBuilderCase)} can only be applied to the {typeof(decimal)} type."));
            }
        }
    }

}
