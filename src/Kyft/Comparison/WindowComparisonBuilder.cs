namespace Kyft;

/// <summary>
/// Builds a staged comparison over recorded window history.
/// </summary>
/// <remarks>
/// The builder captures the comparison question without mutating or enumerating
/// the source history. Later stages add target, comparison sources,
/// normalization, and comparator choices before execution.
/// </remarks>
public sealed class WindowComparisonBuilder
{
    private readonly List<ComparisonSelector> against;
    private readonly List<string> comparators;
    private ComparisonSelector? target;
    private ComparisonScope? scope;
    private ComparisonNormalizationPolicy normalization;
    private ComparisonOutputOptions output;
    private bool isStrict;

    internal WindowComparisonBuilder(WindowIntervalHistory history, string name)
    {
        History = history;
        Name = name;
        this.against = [];
        this.comparators = [];
        this.normalization = ComparisonNormalizationPolicy.Default;
        this.output = ComparisonOutputOptions.Default;
    }

    /// <summary>
    /// Gets the user-supplied comparison name.
    /// </summary>
    public string Name { get; }

    internal WindowIntervalHistory History { get; }

    /// <summary>
    /// Sets the target selector for the comparison.
    /// </summary>
    /// <param name="name">The target name used in output and diagnostics.</param>
    /// <param name="configure">The selector configuration.</param>
    /// <returns>This builder.</returns>
    public WindowComparisonBuilder Target(
        string name,
        Func<ComparisonSelectorBuilder, ComparisonSelector> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        this.target = configure(new ComparisonSelectorBuilder()).WithName(name);
        return this;
    }

    /// <summary>
    /// Adds a comparison selector.
    /// </summary>
    /// <param name="name">The comparison source name used in output and diagnostics.</param>
    /// <param name="configure">The selector configuration.</param>
    /// <returns>This builder.</returns>
    public WindowComparisonBuilder Against(
        string name,
        Func<ComparisonSelectorBuilder, ComparisonSelector> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(configure);

        this.against.Add(configure(new ComparisonSelectorBuilder()).WithName(name));
        return this;
    }

    /// <summary>
    /// Sets the comparison scope.
    /// </summary>
    /// <param name="configure">The scope configuration.</param>
    /// <returns>This builder.</returns>
    public WindowComparisonBuilder Within(Func<ComparisonScopeBuilder, ComparisonScope> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        this.scope = configure(new ComparisonScopeBuilder());
        return this;
    }

    /// <summary>
    /// Sets the normalization policy.
    /// </summary>
    /// <param name="configure">The normalization configuration.</param>
    /// <returns>This builder.</returns>
    public WindowComparisonBuilder Normalize(
        Func<ComparisonNormalizationBuilder, ComparisonNormalizationBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        this.normalization = configure(new ComparisonNormalizationBuilder()).Build();
        return this;
    }

    /// <summary>
    /// Adds comparator declarations.
    /// </summary>
    /// <param name="configure">The comparator configuration.</param>
    /// <returns>This builder.</returns>
    public WindowComparisonBuilder Using(
        Func<ComparisonComparatorBuilder, ComparisonComparatorBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        this.comparators.Clear();
        this.comparators.AddRange(configure(new ComparisonComparatorBuilder()).Build());
        return this;
    }

    /// <summary>
    /// Enables strict validation for later execution stages.
    /// </summary>
    /// <returns>This builder.</returns>
    public WindowComparisonBuilder Strict()
    {
        this.isStrict = true;
        return this;
    }

    /// <summary>
    /// Builds the comparison plan without executing it.
    /// </summary>
    /// <returns>The built comparison plan.</returns>
    public ComparisonPlan Build()
    {
        return new ComparisonPlan(
            Name,
            this.target,
            this.against,
            this.scope,
            this.normalization,
            this.comparators,
            this.output,
            this.isStrict);
    }

    /// <summary>
    /// Validates the current builder state.
    /// </summary>
    /// <returns>The validation diagnostics for the current plan.</returns>
    public IReadOnlyList<ComparisonPlanDiagnostic> Validate()
    {
        return Build().Validate();
    }

    /// <summary>
    /// Prepares the current plan enough to inspect validation diagnostics.
    /// </summary>
    /// <returns>A prepared comparison shell for the current plan.</returns>
    public PreparedComparison Prepare()
    {
        var plan = Build();
        return new PreparedComparison(plan, plan.Validate());
    }

    /// <summary>
    /// Runs the current plan through the available high-level validation stage.
    /// </summary>
    /// <returns>A comparison result shell for the current plan.</returns>
    public ComparisonResult Run()
    {
        var prepared = Prepare();
        return new ComparisonResult(prepared.Plan, prepared.Diagnostics);
    }
}
