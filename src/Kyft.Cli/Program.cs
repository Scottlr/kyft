namespace Kyft.Cli;

internal static class Program
{
    private static int Main(string[] args)
    {
        return KyftCli.Run(args, Console.Out, Console.Error);
    }
}
