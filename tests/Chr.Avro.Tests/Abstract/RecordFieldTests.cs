using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Xunit;

namespace Chr.Avro.Tests
{
    public class RecordFieldTests
    {
        [Fact]
        public void SetsDocumentation()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Null(field.Documentation);

            field.Documentation = "documentation";
            Assert.Equal("documentation", field.Documentation);

            field.Documentation = null;
            Assert.Null(field.Documentation);
        }

        [Theory]
        [MemberData(nameof(ValidNameData))]
        public void SetsName(string name)
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Equal("test", field.Name);

            field.Name = name;
            Assert.Equal(name, field.Name);
        }

        [Fact]
        public void SetsType()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.IsType<NullSchema>(field.Type);

            field.Type = new IntSchema();
            Assert.IsType<IntSchema>(field.Type);
        }

        [Theory]
        [MemberData(nameof(InvalidNameData))]
        public void ThrowsWhenConstructedWithInvalidFieldName(string name)
        {
            Assert.Throws<InvalidNameException>(() => new RecordField(name, new NullSchema()));
        }

        [Fact]
        public void ThrowsWhenNameIsNeverSet()
        {
            var field = (RecordField)FormatterServices.GetUninitializedObject(typeof(RecordField));
            Assert.Throws<InvalidOperationException>(() => field.Name);
        }

        [Fact]
        public void ThrowsWhenNameIsSetToNull()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Throws<ArgumentNullException>(() => field.Name = null);
            Assert.Equal("test", field.Name);
        }

        [Fact]
        public void ThrowsWhenTypeIsNeverSet()
        {
            var field = (RecordField)FormatterServices.GetUninitializedObject(typeof(RecordField));
            Assert.Throws<InvalidOperationException>(() => field.Type);
        }

        [Fact]
        public void ThrowsWhenTypeIsSetToNull()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Throws<ArgumentNullException>(() => field.Type = null);
            Assert.IsType<NullSchema>(field.Type);
        }

        public static IEnumerable<object[]> InvalidNameData => new List<object[]>
        {
            new object[] { "" },
            new object[] { "0name" },
            new object[] { "space space" },
            new object[] { "kebab-case" },
            new object[] { "namespace.name" },
        };

        public static IEnumerable<object[]> ValidNameData => new List<object[]>
        {
            new object[] { "lowercase" },
            new object[] { "UPPERCASE" },
            new object[] { "snake_case" },
            new object[] { "_" },
            new object[] { "name0" },
        };
    }
}
