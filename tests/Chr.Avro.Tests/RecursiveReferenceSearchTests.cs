using System;
using System.Collections.Generic;
using System.Linq;
using Chr.Avro.Abstract;
using Chr.Avro.Infrastructure;
using Xunit;

namespace Chr.Avro.Serialization.Tests;

public class RecursiveReferenceSearchTests
{
    public static TheoryData<Schema, Schema[]> NonRecursiveSchemas
    {
        get
        {
            var data = new TheoryData<Schema, Schema[]>();

            // Primitive
            var booleanSchema = new BooleanSchema();
            data.Add(booleanSchema, new Schema[] { booleanSchema });

            var bytesSchema = new BytesSchema();
            data.Add(bytesSchema, new Schema[] { bytesSchema });

            var doubleSchema = new DoubleSchema();
            data.Add(doubleSchema, new Schema[] { doubleSchema });

            var floatSchema = new FloatSchema();
            data.Add(floatSchema, new Schema[] { floatSchema });

            var intSchema = new IntSchema();
            data.Add(intSchema, new Schema[] { intSchema });

            var longSchema = new LongSchema();
            data.Add(longSchema, new Schema[] { longSchema });

            var nullSchema = new NullSchema();
            data.Add(nullSchema, new Schema[] { nullSchema });

            var stringSchema = new StringSchema();
            data.Add(stringSchema, new Schema[] { stringSchema });

            // Complex
            var boolItem = new BooleanSchema();
            var arrayBool = new ArraySchema(boolItem);
            data.Add(arrayBool, new Schema[] { arrayBool, boolItem });

            var stringItem = new StringSchema();
            var innerArray = new ArraySchema(stringItem);
            var outerArray = new ArraySchema(innerArray);
            data.Add(outerArray, new Schema[] { outerArray, innerArray, stringItem });

            var doubleValue = new DoubleSchema();
            var mapDouble = new MapSchema(doubleValue);
            data.Add(mapDouble, new Schema[] { mapDouble, doubleValue });

            var intValue = new IntSchema();
            var innerMap = new MapSchema(intValue);
            var middleArray = new ArraySchema(innerMap);
            var outerMap = new MapSchema(middleArray);
            data.Add(outerMap, new Schema[] { outerMap, middleArray, innerMap, intValue });

            var emptyUnion = new UnionSchema(Array.Empty<Schema>());
            data.Add(emptyUnion, new Schema[] { emptyUnion });

            var intSchema2 = new IntSchema();
            var unionOne = new UnionSchema(new Schema[] { intSchema2 });
            data.Add(unionOne, new Schema[] { unionOne, intSchema2 });

            var intSchema3 = new IntSchema();
            var stringSchema2 = new StringSchema();
            var unionTwo = new UnionSchema(new Schema[] { intSchema3, stringSchema2 });
            data.Add(unionTwo, new Schema[] { unionTwo, intSchema3, stringSchema2 });

            var intSchema4 = new IntSchema();
            var stringSchema3 = new StringSchema();
            var floatItem = new FloatSchema();
            var arrayFloat = new ArraySchema(floatItem);
            var unionThree = new UnionSchema(new Schema[] { intSchema4, stringSchema3, arrayFloat });
            data.Add(unionThree, new Schema[] { unionThree, intSchema4, stringSchema3, arrayFloat, floatItem });

            // Named
            var enumSchema = new EnumSchema("MyEnum", new string[] { "hello", "world" });
            data.Add(enumSchema, new Schema[] { enumSchema });

            var fixedSchema = new FixedSchema("fixed", 123);
            data.Add(fixedSchema, new Schema[] { fixedSchema });

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(NonRecursiveSchemas), MemberType = typeof(RecursiveReferenceSearchTests))]
    public void IdentifyNonRecursiveSchemas(Schema schema, Schema[] expectedNonRecursive)
    {
        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(schema, results);

        Assert.Equal(expectedNonRecursive.Length, results.Count);
        foreach (var expected in expectedNonRecursive)
        {
            Assert.False(results[expected]);
        }
    }

    [Fact]
    public void IdentifiesImmediateRecursion()
    {
        var record = new RecordSchema("record");
        var intSchema = new IntSchema();
        record.Fields.Add(new RecordField("primitive", intSchema));
        record.Fields.Add(new RecordField("child", record));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Equal(2, results.Count);
        Assert.True(results[record]);
        Assert.False(results[intSchema]);
    }

    [Fact]
    public void IdentifiesImmediateRecursionWithTwoChildren()
    {
        var record = new RecordSchema("record");
        var intSchema = new IntSchema();
        record.Fields.Add(new RecordField("primitive", intSchema));
        record.Fields.Add(new RecordField("child", record));
        record.Fields.Add(new RecordField("child2", record));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Equal(2, results.Count);
        Assert.True(results[record]);
        Assert.False(results[intSchema]);
    }

    [Fact]
    public void IdentifiesNestedRecursion()
    {
        var recursiveRecord = new RecordSchema("a");
        var nullSchema = new NullSchema();
        var unionSchema = new UnionSchema(new Schema[] { recursiveRecord, nullSchema });
        var arraySchema = new ArraySchema(unionSchema);
        var intSchema = new IntSchema();
        recursiveRecord.Fields.Add(new RecordField("b", intSchema));
        var intermediate = new RecordSchema("c");
        intermediate.Fields.Add(new RecordField("d", arraySchema));
        recursiveRecord.Fields.Add(new RecordField("f", intermediate));

        var topLevelRecord = new RecordSchema("g");
        topLevelRecord.Fields.Add(new RecordField("h", recursiveRecord));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(topLevelRecord, results);

        Assert.Equal(7, results.Count);
        Assert.True(results[recursiveRecord]);
        Assert.True(results[intermediate]);
        Assert.True(results[unionSchema]);
        Assert.True(results[arraySchema]);
        Assert.False(results[topLevelRecord]);
        Assert.False(results[nullSchema]);
        Assert.False(results[intSchema]);
    }

    [Fact]
    public void IdentifiesMutualRecursion()
    {
        var recordA = new RecordSchema("A");
        var recordB = new RecordSchema("B");
        recordA.Fields.Add(new RecordField("b", recordB));
        recordB.Fields.Add(new RecordField("a", recordA));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(recordA, results);

        Assert.Equal(2, results.Count);
        Assert.True(results[recordA]);
        Assert.True(results[recordB]);
    }

    [Fact]
    public void IdentifiesRecursionThroughArray()
    {
        var record = new RecordSchema("Node");
        var arraySchema = new ArraySchema(record);
        record.Fields.Add(new RecordField("children", arraySchema));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Equal(2, results.Count);
        Assert.True(results[record]);
        Assert.True(results[arraySchema]);
    }

    [Fact]
    public void IdentifiesRecursionThroughMap()
    {
        var record = new RecordSchema("Node");
        var mapSchema = new MapSchema(record);
        record.Fields.Add(new RecordField("children", mapSchema));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Equal(2, results.Count);
        Assert.True(results[record]);
        Assert.True(results[mapSchema]);
    }

    [Fact]
    public void IdentifiesSharedSchemaWithoutCycle()
    {
        var sharedInt = new IntSchema();
        var recordA = new RecordSchema("A");
        var recordB = new RecordSchema("B");
        recordA.Fields.Add(new RecordField("value", sharedInt));
        recordB.Fields.Add(new RecordField("value", sharedInt));

        var topRecord = new RecordSchema("Top");
        topRecord.Fields.Add(new RecordField("a", recordA));
        topRecord.Fields.Add(new RecordField("b", recordB));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(topRecord, results);

        Assert.Equal(4, results.Count);
        Assert.False(results[topRecord]);
        Assert.False(results[recordA]);
        Assert.False(results[recordB]);
        Assert.False(results[sharedInt]);
    }

    [Fact]
    public void IdentifiesMultipleIndependentCycles()
    {
        var recordA = new RecordSchema("A");
        recordA.Fields.Add(new RecordField("self", recordA));

        var recordB = new RecordSchema("B");
        var recordC = new RecordSchema("C");
        recordB.Fields.Add(new RecordField("c", recordC));
        recordC.Fields.Add(new RecordField("b", recordB));

        var topRecord = new RecordSchema("Top");
        topRecord.Fields.Add(new RecordField("a", recordA));
        topRecord.Fields.Add(new RecordField("b", recordB));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(topRecord, results);

        Assert.Equal(4, results.Count);
        Assert.True(results[recordA]);
        Assert.True(results[recordB]);
        Assert.True(results[recordC]);
        Assert.False(results[topRecord]);
    }

    [Fact]
    public void IdentifiesNonRecursiveRecord()
    {
        var record = new RecordSchema("Person");
        var nameSchema = new StringSchema();
        var ageSchema = new IntSchema();
        record.Fields.Add(new RecordField("name", nameSchema));
        record.Fields.Add(new RecordField("age", ageSchema));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Equal(3, results.Count);
        Assert.False(results[record]);
        Assert.False(results[nameSchema]);
        Assert.False(results[ageSchema]);
    }

    [Fact]
    public void IdentifiesEmptyRecord()
    {
        var record = new RecordSchema("Empty");

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Single(results);
        Assert.False(results[record]);
    }

    [Fact]
    public void IdentifiesRecursionThroughUnion()
    {
        var record = new RecordSchema("Node");
        var nullSchema = new NullSchema();
        var unionSchema = new UnionSchema(new Schema[] { record, nullSchema });
        record.Fields.Add(new RecordField("next", unionSchema));

        var results = new Dictionary<Schema, bool>();
        RecursiveReferenceSearch.Collect(record, results);

        Assert.Equal(3, results.Count);
        Assert.True(results[record]);
        Assert.True(results[unionSchema]);
        Assert.False(results[nullSchema]);
    }
}
