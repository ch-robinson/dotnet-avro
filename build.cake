#tool nuget:?package=mdoc&version=5.7.4.9

var ROOT = ".";

var MDOC_PATH = $"{ROOT}/docs/api";
var RELEASES_PATH = $"{ROOT}/releases";
var SLN_PATH = $"{ROOT}/Chr.Avro.sln";
var SRC_PATH = $"{ROOT}/src";
var TESTS_PATH = $"{ROOT}/tests";

var configuration = Argument<string>("configuration", "Release");
var target = Argument("target", "Default");

Task("Build")
    .IsDependentOn("Clean")
    .Does(() =>
    {
        foreach (var info in GetFiles($"{SRC_PATH}/**/*.csproj"))
        {
            DotNetCoreBuild(info.FullPath, new DotNetCoreBuildSettings()
            {
                Configuration = configuration
            });
        }
    });

Task("Clean")
    .Does(() =>
    {
        CleanDirectories(MDOC_PATH);

        CleanDirectories(RELEASES_PATH);

        CleanDirectories($"{SRC_PATH}/**/bin");
        CleanDirectories($"{SRC_PATH}/**/obj");

        CleanDirectories($"{TESTS_PATH}/**/bin");
        CleanDirectories($"{TESTS_PATH}/**/obj");
    });

Task("Default")
    .IsDependentOn("Build")
    .IsDependentOn("Test");

Task("Docs")
    .IsDependentOn("Build")
    .Does(() =>
    {
        var mdoc = Context.Tools.Resolve("mdoc.exe");

        // whitelist for now:
        var names = new[]
        {
            "Chr.Avro",
            "Chr.Avro.Binary",
            "Chr.Avro.Confluent",
            "Chr.Avro.Json"
        };

        var arguments = new List<string>()
            .Concat(names.Select(n => $"-i {SRC_PATH}/{n}/bin/{configuration}/netstandard2.0/{n}.xml"))
            .Concat(names.Select(n => $"-L {SRC_PATH}/{n}/bin/{configuration}/netstandard2.0"))
            .Concat(names.Select(n => $"{SRC_PATH}/{n}/bin/{configuration}/netstandard2.0/{n}.dll"));

        StartProcess(mdoc, new ProcessSettings
        {
            Arguments = $"update --debug --delete -o '{MDOC_PATH}' {string.Join(" ", arguments)}"
        });
    });

Task("Pack")
    .IsDependentOn("Clean")
    .IsDependentOn("Test")
    .Does(() =>
    {
        foreach (var info in GetFiles($"{SRC_PATH}/**/*.csproj"))
        {
            DotNetCorePack(info.FullPath, new DotNetCorePackSettings()
            {
                Configuration = configuration,
                OutputDirectory = RELEASES_PATH
            });
        }
    });

Task("Publish")
    .IsDependentOn("Pack")
    .Does(() =>
    {
        var pattern = @"(?<name>.+)\.(?<version>[0-9]+\.[0-9]+\.[0-9]+.*)\.nupkg";

        foreach (var info in GetFiles($"{RELEASES_PATH}/*.nupkg"))
        {
            var package = info.GetFilename().FullPath;
            var match = System.Text.RegularExpressions.Regex.Match(package, pattern);

            if (!match.Success)
            {
                Error($"Could not determine a name and version for {package}.");
                continue;
            }

            var name = match.Groups["name"].Value;

            NuGetPush(info.FullPath, new NuGetPushSettings());
        }
    });

Task("Test")
    .IsDependentOn("Clean")
    .Does(() =>
    {
        foreach (var info in GetFiles($"{TESTS_PATH}/**/*.csproj"))
        {
            DotNetCoreTest(info.FullPath);
        }
    });

RunTarget(target);
