using System.Xml.Linq;

namespace Kyft.Tests.Setup;

public sealed class ApiFreezeReadinessTests
{
    [Theory]
    [InlineData("src/Kyft/Kyft.csproj")]
    [InlineData("src/Kyft.Testing/Kyft.Testing.csproj")]
    public void PackableProjectsEnforcePublicXmlDocumentation(string projectPath)
    {
        var project = LoadProject(projectPath);

        Assert.Equal("true", GetProperty(project, "GenerateDocumentationFile"));
        Assert.Contains("CS1591", GetProperty(project, "WarningsAsErrors"), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("src/Kyft/Kyft.csproj", "true")]
    [InlineData("src/Kyft.Testing/Kyft.Testing.csproj", "true")]
    [InlineData("src/Kyft.Cli/Kyft.Cli.csproj", "false")]
    [InlineData("benchmarks/Kyft.Benchmarks/Kyft.Benchmarks.csproj", "false")]
    public void PackageBoundariesAreExplicit(string projectPath, string expectedPackable)
    {
        var project = LoadProject(projectPath);

        var isPackable = GetProperty(project, "IsPackable");
        if (string.IsNullOrEmpty(isPackable) && projectPath == "src/Kyft/Kyft.csproj")
        {
            isPackable = "true";
        }

        Assert.Equal(expectedPackable, isPackable);
    }

    private static XDocument LoadProject(string projectPath)
    {
        return XDocument.Load(Path.Combine(RepositoryRoot(), projectPath));
    }

    private static string GetProperty(XDocument project, string name)
    {
        return project.Root!
            .Elements("PropertyGroup")
            .Elements(name)
            .Select(static element => element.Value)
            .FirstOrDefault() ?? string.Empty;
    }

    private static string RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "Kyft.slnx")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("Repository root was not found.");
    }
}
