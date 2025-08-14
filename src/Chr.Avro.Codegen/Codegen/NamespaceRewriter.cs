namespace Chr.Avro.Codegen
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.CodeAnalysis;
    using Microsoft.CodeAnalysis.CSharp;
    using Microsoft.CodeAnalysis.CSharp.Syntax;

    /// <summary>
    /// A <see cref="CSharpSyntaxRewriter" /> that simplifies namespaces in generated code.
    /// </summary>
    internal class NamespaceRewriter : CSharpSyntaxRewriter
    {
        private readonly Stack<string> breadcrumb;

        private ISet<string>? externals;

        private ISet<string>? internals;

        private NamespaceRewriter()
        {
            breadcrumb = new Stack<string>();
        }

        /// <inheritdoc />
        public override SyntaxNode VisitCompilationUnit(CompilationUnitSyntax node)
        {
            var descendants = node.DescendantNodesAndSelf();

            var parameters = descendants.OfType<ParameterSyntax>().Select(p => p.Type);
            var properties = descendants.OfType<PropertyDeclarationSyntax>().Select(p => p.Type);

            internals = new HashSet<string>(descendants
                .OfType<NamespaceDeclarationSyntax>()
                .Select(n => n.Name.ToString()));

            externals = new HashSet<string>(Enumerable
                .Concat(parameters, properties)
                .OfType<QualifiedNameSyntax>()
                .Select(n => StripGlobalAlias(n.Left).ToString())
                .Where(n => !internals.Contains(n)));

            var result = ((CompilationUnitSyntax)base.VisitCompilationUnit(node)!)
                .WithUsings(new SyntaxList<UsingDirectiveSyntax>(externals
                    .Select(n => SyntaxFactory.UsingDirective(SyntaxFactory.ParseName(n)))));

            externals = null;
            internals = null;

            return result;
        }

        /// <inheritdoc />
        public override SyntaxNode VisitNamespaceDeclaration(NamespaceDeclarationSyntax node)
        {
            breadcrumb.Push(node.Name.ToString());
            var result = base.VisitNamespaceDeclaration(node)!;
            breadcrumb.Pop();

            return result;
        }

        /// <inheritdoc />
        public override SyntaxNode? VisitParameter(ParameterSyntax node)
        {
            var result = (ParameterSyntax)base.VisitParameter(node)!;

            if (result.Type is NameSyntax name)
            {
                result = result.WithType(Reduce(name)).WithTriviaFrom(result);
            }

            return result;
        }

        /// <inheritdoc />
        public override SyntaxNode VisitPropertyDeclaration(PropertyDeclarationSyntax node)
        {
            var result = (PropertyDeclarationSyntax)base.VisitPropertyDeclaration(node)!;

            if (result.Type is NameSyntax name)
            {
                result = result.WithType(Reduce(name));
            }

            return result;
        }

        /// <inheritdoc />
        public override SyntaxNode VisitTypeArgumentList(TypeArgumentListSyntax node)
        {
            var result = (TypeArgumentListSyntax)base.VisitTypeArgumentList(node)!;

            // VisitQualifiedName doesn’t hit these
            var children = node.ChildNodes().OfType<NameSyntax>();

            if (children.Any())
            {
                result = result.ReplaceNodes(children, (child, rewritten) => Reduce(child));
            }

            return result;
        }

        /// <summary>
        /// Simplifies namespaces in a compilation unit.
        /// </summary>
        /// <param name="unit">
        /// A compilation unit to simplify.
        /// </param>
        /// <returns>
        /// A compilation unit with namespaces simplified.
        /// </returns>
        internal static CompilationUnitSyntax Rewrite(CompilationUnitSyntax unit)
        {
            return ((CompilationUnitSyntax)new NamespaceRewriter().Visit(unit))!;
        }

        private static NameSyntax StripGlobalAlias(NameSyntax name)
        {
            switch (name)
            {
                case AliasQualifiedNameSyntax alias:
                    if (alias.Alias.Identifier.IsKind(SyntaxKind.GlobalKeyword))
                    {
                        return alias.Name;
                    }

                    throw new NotImplementedException();

                case QualifiedNameSyntax qualified:
                    return qualified.WithLeft(StripGlobalAlias(qualified.Left));

                default:
                    return name;
            }
        }

        private NameSyntax Reduce(NameSyntax name)
        {
            if (externals == null || internals == null)
            {
                throw new InvalidOperationException("Rewriting can only be done within a compilation unit.");
            }

            name = StripGlobalAlias(name);

            switch (name)
            {
                case QualifiedNameSyntax qualified:
                    if (externals.Contains(qualified.Left.ToString()))
                    {
                        return qualified.Right;
                    }

                    var output = qualified.ToString();
                    var qualifiers = breadcrumb.Reverse().SelectMany(c => c.Split('.'));

                    foreach (var qualifier in qualifiers)
                    {
                        if (output.StartsWith($"{qualifier}."))
                        {
                            output = output.Substring(qualifier.Length + 1);
                        }
                        else
                        {
                            break;
                        }
                    }

                    return SyntaxFactory.ParseName(output);

                default:
                    return name;
            }
        }
    }
}
