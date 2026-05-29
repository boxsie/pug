namespace PUG.Core.Tests;

public sealed class InMemoryPrivateLobbyTests
{
    [Fact]
    public async Task GenerateCodeAsync_ReturnsValidCodeAndId()
    {
        var lobby = new InMemoryPrivateLobby();
        var allowed = new HashSet<char>(ShortCodeGenerator.Alphabet);

        var (code, id) = await lobby.GenerateCodeAsync(CancellationToken.None);

        Assert.Equal(6, code.Length);
        Assert.All(code, c => Assert.Contains(c, allowed));
        Assert.NotEqual(Guid.Empty, id);
    }

    [Fact]
    public async Task ResolveCodeAsync_KnownCode_ReturnsStoredGuid()
    {
        var lobby = new InMemoryPrivateLobby();
        var (code, id) = await lobby.GenerateCodeAsync(CancellationToken.None);

        var resolved = await lobby.ResolveCodeAsync(code, CancellationToken.None);

        Assert.Equal(id, resolved);
    }

    [Fact]
    public async Task ResolveCodeAsync_UnknownCode_ReturnsNull()
    {
        var lobby = new InMemoryPrivateLobby();

        Assert.Null(await lobby.ResolveCodeAsync("XXXXXX", CancellationToken.None));
    }

    [Fact]
    public async Task GenerateCodeAsync_TwiceProducesDistinctCodesAndIds()
    {
        var lobby = new InMemoryPrivateLobby();
        var first = await lobby.GenerateCodeAsync(CancellationToken.None);
        var second = await lobby.GenerateCodeAsync(CancellationToken.None);

        Assert.NotEqual(first.Code, second.Code);
        Assert.NotEqual(first.PrivateGameId, second.PrivateGameId);
    }

    [Fact]
    public async Task ExpireCodeAsync_FollowedByResolve_ReturnsNull()
    {
        var lobby = new InMemoryPrivateLobby();
        var (code, _) = await lobby.GenerateCodeAsync(CancellationToken.None);

        await lobby.ExpireCodeAsync(code, CancellationToken.None);

        Assert.Null(await lobby.ResolveCodeAsync(code, CancellationToken.None));
    }

    [Fact]
    public async Task ExpireCodeAsync_UnknownCode_NoThrow()
    {
        var lobby = new InMemoryPrivateLobby();

        await lobby.ExpireCodeAsync("UNKNOWN", CancellationToken.None);
    }

    [Fact]
    public async Task ResolveCodeAsync_NullCode_Throws()
    {
        var lobby = new InMemoryPrivateLobby();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            lobby.ResolveCodeAsync(null!, CancellationToken.None));
    }
}
