using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Chr.Avro.Codegen
{
    internal class NamespaceRewriter : CSharpSyntaxRewriter
    {
        private readonly Stack<string> _breadcrumb;

        private ISet<string>? externals;

        private ISet<string>? internals;

        private NamespaceRewriter()
        {
            _breadcrumb = new Stack<string>();
        }

        public override SyntaxNode VisitCompilationUnit(CompilationUnitSyntax node)
        {
            var descendants = node.DescendantNodesAndSelf();

            internals = new HashSet<string>(descendants
                .OfType<NamespaceDeclarationSyntax>()
                .Select(n => n.Name.ToString()));

            externals = new HashSet<string>(descendants
                .OfType<PropertyDeclarationSyntax>()
                .Select(p => p.Type)
                .OfType<QualifiedNameSyntax>()
                .Select(n => StripGlobalAlias(n.Left).ToString())
                .Where(n => !internals.Contains(n)));

            var result = (base.VisitCompilationUnit(node) as CompilationUnitSyntax)!
                .WithUsings(new SyntaxList<UsingDirectiveSyntax>(externals
                    .Select(n => SyntaxFactory.UsingDirective(SyntaxFactory.ParseName(n)))));

            externals = null;
            internals = null;

            return result;
        }

        public override SyntaxNode VisitNamespaceDeclaration(NamespaceDeclarationSyntax node)
        {
            _breadcrumb.Push(node.Name.ToString());
            var result = base.VisitNamespaceDeclaration(node)!;
            _breadcrumb.Pop();

            return result;
        }

        public override SyntaxNode VisitPropertyDeclaration(PropertyDeclarationSyntax node)
        {
            var result = (base.VisitPropertyDeclaration(node) as PropertyDeclarationSyntax)!;

            if (result.Type is NameSyntax name)
            {
                result = result.WithType(Reduce(name));
            }

            return result;
        }

        public override SyntaxNode VisitTypeArgumentList(TypeArgumentListSyntax node)
        {
            var result = (base.VisitTypeArgumentList(node) as TypeArgumentListSyntax)!;

            // VisitQualifiedName doesn’t hit these
            var children = node.ChildNodes().OfType<NameSyntax>();

            if (children.Count() > 0)
            {
                result = result.ReplaceNodes(children, (child, rewritten) => Reduce(child));
            }

            return result;
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
                    var qualifiers = _breadcrumb.Reverse().SelectMany(c => c.Split('.'));

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

        internal static CompilationUnitSyntax Rewrite(CompilationUnitSyntax unit)
        {
            return (new NamespaceRewriter().Visit(unit) as CompilationUnitSyntax)!;
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
    }

    internal static partial class SyntaxNodeExtensions
    {
        public static CompilationUnitSyntax RewriteNamespaces(this CompilationUnitSyntax node)
        {
            return NamespaceRewriter.Rewrite(node);
        }
    }
}
