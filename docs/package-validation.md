# Package Validation

Kyft package checks are designed to stay local until publish automation exists.

Run the package build:

```bash
dotnet pack src/Kyft/Kyft.csproj -c Release -o artifacts/package # Build the NuGet package locally.
```

Inspect the package contents:

```bash
unzip -l artifacts/package/Kyft.0.1.0.nupkg # Inspect library package contents.
unzip -l artifacts/package/Kyft.0.1.0.snupkg # Inspect symbol package contents.
```

Expected package contents include:

- `lib/net8.0/Kyft.dll`
- `lib/net8.0/Kyft.xml`
- `lib/net10.0/Kyft.dll`
- `lib/net10.0/Kyft.xml`
- `README.md`

Run a consumer smoke test from a temporary directory:

```bash
dotnet new console --framework net8.0 # Create a temporary consumer app.
dotnet add package Kyft --version 0.1.0 --source /absolute/path/to/kyft/artifacts/package # Reference the local package.
dotnet build # Verify the consumer can restore and compile.
```

Package validation is enabled in the project. Source Link and symbol packages
are produced for local verification, while public publishing remains a separate
release step.
