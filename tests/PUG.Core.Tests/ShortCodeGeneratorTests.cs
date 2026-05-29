namespace PUG.Core.Tests;

public sealed class ShortCodeGeneratorTests
{
    [Fact]
    public void Generate_OnlyAlphabetChars()
    {
        var allowed = new HashSet<char>(ShortCodeGenerator.Alphabet);

        for (var i = 0; i < 100; i++)
        {
            var code = ShortCodeGenerator.Generate();
            foreach (var c in code)
            {
                Assert.Contains(c, allowed);
            }
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(32)]
    public void Generate_LengthMatchesRequest(int length)
    {
        Assert.Equal(length, ShortCodeGenerator.Generate(length).Length);
    }

    [Fact]
    public void Generate_ZeroLength_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ShortCodeGenerator.Generate(0));
    }

    [Fact]
    public void Generate_NegativeLength_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ShortCodeGenerator.Generate(-1));
    }

    [Fact]
    public void Generate_DistributionIsRoughlyUniform()
    {
        // Non-strict sanity check: with a 32-char alphabet and a power-of-two
        // mod, each char should land ~ N/32 of the time. Bounds are generous
        // (±50%) so the test isn't flaky on a real CSPRNG.
        const int n = 32_000;
        var counts = new Dictionary<char, int>();
        foreach (var c in ShortCodeGenerator.Alphabet)
        {
            counts[c] = 0;
        }

        for (var i = 0; i < n; i++)
        {
            counts[ShortCodeGenerator.Generate(1)[0]] += 1;
        }

        var expected = n / ShortCodeGenerator.Alphabet.Length;
        foreach (var kv in counts)
        {
            Assert.InRange(kv.Value, (int)(expected * 0.5), (int)(expected * 1.5));
        }
    }

    [Fact]
    public async Task GenerateUniqueAsync_RetriesUntilUnused()
    {
        var calls = 0;
        var unique = await ShortCodeGenerator.GenerateUniqueAsync(
            length: 6,
            isUsed: _ =>
            {
                calls += 1;
                return Task.FromResult(calls < 3); // first two are "used"
            },
            maxAttempts: 5);

        Assert.Equal(6, unique.Length);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task GenerateUniqueAsync_AllCollisions_Throws()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ShortCodeGenerator.GenerateUniqueAsync(
                length: 6,
                isUsed: _ => Task.FromResult(true),
                maxAttempts: 3));
    }

    [Fact]
    public async Task GenerateUniqueAsync_NullPredicate_Throws()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            ShortCodeGenerator.GenerateUniqueAsync(6, null!, 3));
    }

    [Fact]
    public async Task GenerateUniqueAsync_BadMaxAttempts_Throws()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            ShortCodeGenerator.GenerateUniqueAsync(6, _ => Task.FromResult(false), 0));
    }
}
