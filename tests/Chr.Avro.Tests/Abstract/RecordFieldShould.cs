namespace Chr.Avro.Tests
{
    using System;
    using Chr.Avro.Abstract;
    using Chr.Avro.Fixtures;
    using Chr.Avro.Infrastructure;
    using Xunit;

    public class RecordFieldShould
    {
        [Fact]
        public void SetDefault()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Null(field.Default);

            var defaultValue = new ObjectDefaultValue<object>(null, field.Type);
            field.Default = defaultValue;
            Assert.Equal(defaultValue, field.Default);

            field.Default = null;
            Assert.Null(field.Default);
        }

        [Fact]
        public void SetDocumentation()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Null(field.Documentation);

            field.Documentation = "documentation";
            Assert.Equal("documentation", field.Documentation);

            field.Documentation = null;
            Assert.Null(field.Documentation);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.ValidIdentifiers), MemberType = typeof(IdentifierData))]
        public void SetName(string name)
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Equal("test", field.Name);

            field.Name = name;
            Assert.Equal(name, field.Name);
        }

        [Fact]
        public void SetType()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.IsType<NullSchema>(field.Type);

            field.Type = new IntSchema();
            Assert.IsType<IntSchema>(field.Type);
        }

        [Theory]
        [MemberData(nameof(IdentifierData.InvalidIdentifiers), MemberType = typeof(IdentifierData))]
        public void ThrowWhenConstructedWithInvalidFieldName(string name)
        {
            Assert.Throws<InvalidNameException>(() => new RecordField(name, new NullSchema()));
        }

        [Fact]
        public void ThrowWhenNameIsNeverSet()
        {
            var field = ReflectionExtensions.GetUninitializedInstance<RecordField>();
            Assert.Throws<InvalidOperationException>(() => field.Name);
        }

        [Fact]
        public void ThrowWhenNameIsSetToNull()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Throws<ArgumentNullException>(() => field.Name = null);
            Assert.Equal("test", field.Name);
        }

        [Fact]
        public void ThrowWhenTypeIsNeverSet()
        {
            var field = ReflectionExtensions.GetUninitializedInstance<RecordField>();
            Assert.Throws<InvalidOperationException>(() => field.Type);
        }

        [Fact]
        public void ThrowWhenTypeIsSetToNull()
        {
            var field = new RecordField("test", new NullSchema());
            Assert.Throws<ArgumentNullException>(() => field.Type = null);
            Assert.IsType<NullSchema>(field.Type);
        }
    }
}
