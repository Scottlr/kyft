namespace Spanfold.Cli;

internal static class Program
{
    private static int Main(string[] args)
    {
        return SpanfoldCli.Run(args, Console.Out, Console.Error);
    }
}
