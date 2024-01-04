namespace Chr.Avro.Codegen
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using Chr.Avro.Abstract;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.CSharp.Syntax;
    using Microsoft.CodeAnalysis.Formatting;

    /// <summary>
    /// Generates C# classes and enums that match Avro schemas.
    /// </summary>
    public class CSharpCodeGenerator : ICodeGenerator
    {
        private readonly bool enableNullableReferenceTypes;
        private readonly bool enableDescriptionAttributeForDocumentation;

        /// <summary>
        /// Initializes a new instance of the <see cref="CSharpCodeGenerator" /> class.
        /// </summary>
        /// <param name="enableNullableReferenceTypes">
        /// Whether reference types selected for nullable record fields should be annotated as
        /// nullable.
        /// </param>
        /// <param name="enableDescriptionAttributeForDocumentation">
        /// Whether enum and record schema documentation should be reflected in
        /// <see cref="System.ComponentModel.DescriptionAttribute" />s on types and members.
        /// </param>
        public CSharpCodeGenerator(bool enableNullableReferenceTypes = true, bool enableDescriptionAttributeForDocumentation = false)
        {
            this.enableNullableReferenceTypes = enableNullableReferenceTypes;
            this.enableDescriptionAttributeForDocumentation = enableDescriptionAttributeForDocumentation;
        }

        /// <summary>
        /// Generates a class declaration for a record schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate a class for.
        /// </param>
        /// <returns>
        /// A class declaration with a property for each field of the record schema.
        /// </returns>
        /// <throws cref="UnsupportedSchemaException">
        /// Thrown when a field schema is not recognized.
        /// </throws>
        public virtual ClassDeclarationSyntax GenerateClass(RecordSchema schema)
        {
            var declaration = SyntaxFactory.ClassDeclaration(schema.Name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddMembers(schema.Fields
                    .Select(field =>
                    {
                        var child = SyntaxFactory
                            .PropertyDeclaration(
                                GetPropertyType(field.Type),
                                field.Name)
                            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                            .AddAccessorListAccessors(
                                SyntaxFactory.AccessorDeclaration(SyntaxKind.GetAccessorDeclaration)
                                    .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)),
                                SyntaxFactory.AccessorDeclaration(SyntaxKind.SetAccessorDeclaration)
                                    .WithSemicolonToken(SyntaxFactory.Token(SyntaxKind.SemicolonToken)))
                            .AddAttributeLists(GetDescriptionAttribute(field.Documentation));

                        if (!string.IsNullOrEmpty(field.Documentation))
                        {
                            child = AddSummaryComment(child, field.Documentation!);
                        }

                        return child;
                    })
                    .Where(field => field != null)
                    .ToArray())
                .AddAttributeLists(GetDescriptionAttribute(schema.Documentation));

            if (!string.IsNullOrEmpty(schema.Documentation))
            {
                declaration = AddSummaryComment(declaration, schema.Documentation!);
            }

            return declaration;
        }

        /// <summary>
        /// Generates an enum declaration for an enum schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate an enum for.
        /// </param>
        /// <returns>
        /// An enum declaration with members that match the symbols of the enum schema.
        /// </returns>
        public virtual EnumDeclarationSyntax GenerateEnum(EnumSchema schema)
        {
            var declaration = SyntaxFactory.EnumDeclaration(schema.Name)
                .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
                .AddMembers(schema.Symbols
                    .Select(symbol => SyntaxFactory.EnumMemberDeclaration(symbol))
                    .ToArray())
                .AddAttributeLists(GetDescriptionAttribute(schema.Documentation));

            if (!string.IsNullOrEmpty(schema.Documentation))
            {
                declaration = AddSummaryComment(declaration, schema.Documentation!);
            }

            return declaration;
        }

        /// <summary>
        /// Generates a compilation unit (essentially a single .cs file) that contains types that
        /// match the schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate code for. Code can only be generated for enum
        /// and record schemas.
        /// </param>
        /// <returns>
        /// A compilation unit containing types that match the schema.
        /// </returns>
        /// <throws cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an enum or record, or when a record
        /// field schema is not recognized.
        /// </throws>
        public virtual CompilationUnitSyntax GenerateCompilationUnit(Schema schema)
        {
            var candidates = GetCandidateSchemas(schema)
                .OrderBy(s => s.Name)
                .GroupBy(s => s.Namespace)
                .OrderBy(g => g.Key);

            if (!candidates.Any())
            {
                throw new UnsupportedSchemaException(schema, $"Code can only be generated for enums and records.");
            }

            var unit = SyntaxFactory.CompilationUnit();

            foreach (var group in candidates)
            {
                var members = group
                    .Select(candidate => candidate switch
                    {
                        EnumSchema enumSchema => GenerateEnum(enumSchema) as MemberDeclarationSyntax,
                        RecordSchema recordSchema => GenerateClass(recordSchema) as MemberDeclarationSyntax,
                        _ => default,
                    })
                    .OfType<MemberDeclarationSyntax>()
                    .ToArray();

                if (group.Key is string key)
                {
                    members = new[]
                    {
                        SyntaxFactory.NamespaceDeclaration(SyntaxFactory.ParseName(key)).AddMembers(members),
                    };
                }

                unit = unit.AddMembers(members);
            }

            return NamespaceRewriter.Rewrite(unit);
        }

        /// <summary>
        /// Writes a compilation unit (essentially a single .cs file) that contains types that
        /// match the schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate code for.
        /// </param>
        /// <param name="stream">
        /// A stream to write the resulting compilation unit to.
        /// </param>
        public void WriteCompilationUnit(Schema schema, Stream stream)
        {
            using var workspace = new AdhocWorkspace();
            using var writer = new StreamWriter(stream);

            var unit = GenerateCompilationUnit(schema) as SyntaxNode;
            unit = Formatter.Format(unit, workspace);

            unit.WriteTo(writer);
        }

        /// <summary>
        /// Writes a compilation unit (essentially a single .cs file) that contains types that
        /// match the schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate code for.
        /// </param>
        /// <returns>
        /// The compilation unit as a string.
        /// </returns>
        public string WriteCompilationUnit(Schema schema)
        {
            var stream = new MemoryStream();

            using (stream)
            {
                WriteCompilationUnit(schema, stream);
            }

            return Encoding.UTF8.GetString(stream.ToArray());
        }

        /// <summary>
        /// Gets a matching type for a schema.
        /// </summary>
        /// <remarks>
        /// Namespaces are prefixed with the global namespace alias. Names can be simplified later on.
        /// </remarks>
        /// <param name="schema">
        /// The schema to match to a type.
        /// </param>
        /// <param name="nullable">
        /// Whether the type should be nullable.
        /// </param>
        /// <returns>
        /// A type that matches the schema.
        /// </returns>
        /// <throws cref="UnsupportedSchemaException">
        /// Thrown when the schema is not recognized.
        /// </throws>
        protected virtual TypeSyntax GetPropertyType(Schema schema, bool nullable = false)
        {
            var type = SyntaxFactory.ParseTypeName("object");
            var value = false;

            switch (schema)
            {
                case BytesSchema b when b.LogicalType is DecimalLogicalType:
                case FixedSchema f when f.LogicalType is DecimalLogicalType:
                    type = SyntaxFactory.ParseTypeName("decimal");
                    value = true;
                    break;

                case FixedSchema f when f.LogicalType is DurationLogicalType:
                    type = SyntaxFactory.ParseTypeName("global::System.TimeSpan");
                    value = true;
                    break;

                case LongSchema l when l.LogicalType is TimestampLogicalType t:
                    type = SyntaxFactory.ParseTypeName("global::System.DateTime");
                    value = true;
                    break;

                case StringSchema s when s.LogicalType is UuidLogicalType:
                    type = SyntaxFactory.ParseTypeName("global::System.Guid");
                    value = true;
                    break;

                case ArraySchema a:
                    type = SyntaxFactory.ParseTypeName($"global::System.Collections.Generic.IEnumerable<{GetPropertyType(a.Item)}>");
                    break;

                case BooleanSchema b:
                    type = SyntaxFactory.ParseTypeName("bool");
                    value = true;
                    break;

                case BytesSchema b:
                case FixedSchema f:
                    type = SyntaxFactory.ParseTypeName("byte[]");
                    break;

                case DoubleSchema d:
                    type = SyntaxFactory.ParseTypeName("double");
                    value = true;
                    break;

                case EnumSchema e:
                    type = SyntaxFactory.ParseTypeName($"global::{e.FullName}");
                    value = true;
                    break;

                case FloatSchema f:
                    type = SyntaxFactory.ParseTypeName("float");
                    value = true;
                    break;

                case IntSchema i:
                    type = SyntaxFactory.ParseTypeName("int");
                    value = true;
                    break;

                case LongSchema l:
                    type = SyntaxFactory.ParseTypeName("long");
                    value = true;
                    break;

                case MapSchema m:
                    type = SyntaxFactory.ParseTypeName($"global::System.Collections.Generic.IDictionary<string, {GetPropertyType(m.Value)}>");
                    break;

                case NullSchema n:
                    break;

                case RecordSchema r:
                    type = SyntaxFactory.ParseTypeName($"global::{r.FullName}");
                    break;

                case StringSchema s:
                    type = SyntaxFactory.ParseTypeName("string");
                    break;

                case UnionSchema u:
                    var nulls = u.Schemas.OfType<NullSchema>();
                    var others = u.Schemas.Except(nulls);

                    try
                    {
                        return GetPropertyType(others.Single(), nulls.Any());
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedSchemaException(u, $"Could not generate a type for the union [{string.Join(", ", u.Schemas.Select(s => s.GetType().Name))}]", exception);
                    }

                default:
                    throw new UnsupportedSchemaException(schema, $"{schema.GetType()} is not recognized by the code generator.");
            }

            if (nullable && (enableNullableReferenceTypes || value))
            {
                type = SyntaxFactory.NullableType(type);
            }

            return type;
        }

        private static TSyntax AddSummaryComment<TSyntax>(TSyntax node, string summary)
            where TSyntax : SyntaxNode
        {
            var components = new XmlNodeSyntax[]
            {
                SyntaxFactory.XmlSummaryElement(SyntaxFactory.XmlText(summary)),
            };

            var trivia = node.GetLeadingTrivia().Add(
                SyntaxFactory.Trivia(
                    SyntaxFactory.DocumentationCommentTrivia(SyntaxKind.MultiLineDocumentationCommentTrivia, SyntaxFactory.List(components))
                        .WithLeadingTrivia(SyntaxFactory.DocumentationCommentExterior("/// "))
                        .WithTrailingTrivia(SyntaxFactory.CarriageReturnLineFeed)));

            return node.WithLeadingTrivia(trivia);
        }

        private static IEnumerable<NamedSchema> GetCandidateSchemas(Schema schema, ISet<Schema>? seen = null)
        {
            seen ??= new HashSet<Schema>();

            if (seen.Add(schema))
            {
                switch (schema)
                {
                    case ArraySchema a:
                        GetCandidateSchemas(a.Item, seen);
                        break;

                    case MapSchema m:
                        GetCandidateSchemas(m.Value, seen);
                        break;

                    case RecordSchema r:
                        foreach (var field in r.Fields)
                        {
                            GetCandidateSchemas(field.Type, seen);
                        }

                        break;

                    case UnionSchema u:
                        foreach (var child in u.Schemas)
                        {
                            GetCandidateSchemas(child, seen);
                        }

                        break;
                }
            }

            return seen.OfType<NamedSchema>();
        }

        private AttributeListSyntax[] GetDescriptionAttribute(string? documentation)
        {
            if (documentation == null || string.IsNullOrEmpty(documentation) || !enableDescriptionAttributeForDocumentation)
            {
                return Array.Empty<AttributeListSyntax>();
            }

            // Generates: [Description("documentation")]
            // https://stackoverflow.com/questions/35927427/how-to-create-an-attributesyntax-with-a-parameter
            var name = SyntaxFactory.ParseName("System.ComponentModel.DescriptionAttribute");
            var arguments = SyntaxFactory.AttributeArgumentList(
                                SyntaxFactory.SingletonSeparatedList(
                                    SyntaxFactory.AttributeArgument(
                                        SyntaxFactory.LiteralExpression(
                                            SyntaxKind.StringLiteralExpression,
                                            SyntaxFactory.Literal(documentation)))));

            var attribute = SyntaxFactory.Attribute(name, arguments);

            var attributeList = default(SeparatedSyntaxList<AttributeSyntax>);
            attributeList = attributeList.Add(attribute);
            var list = SyntaxFactory.AttributeList(attributeList);
            return new AttributeListSyntax[1] { list };
        }
    }
}
