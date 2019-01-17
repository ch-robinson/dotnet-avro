# Chr.Avro

Chr.Avro is an Avro implementation for .NET. It’s designed to serve as a flexible alternative to the [Apache implementation](https://github.com/apache/avro/tree/master/lang/csharp/src/apache/main) and integrate seamlessly with [Confluent’s Kafka and Schema Registry clients](https://github.com/confluentinc/confluent-kafka-dotnet).

## Development

[Cake](https://cakebuild.net) handles all build tasks. Use [**build.ps1**](build.ps1) on Windows and [**build.sh**](build.sh) on macOS and Linux. (Some projects target .NET Framework 4.5.2, so certain tasks won’t work on non-Windows machines.)

The following targets are supported:

| Name        | Description                                     |
|-------------|------------------------------------------------ |
| **Build**   | builds the library projects                     |
| **Clean**   | removes all build and release artifacts         |
| **Pack**    | creates NuGet packages for the library projects |
| **Publish** | pushes packages to NuGet                        |
| **Test**    | runs the test projects                          |

**Build** and **Test** will run if no target is specified.
