using System.Security.Cryptography;

namespace PUG.Core;

/// <summary>
/// Generates human-readable short codes for private rooms.
/// </summary>
/// <remarks>
/// <para>
/// The alphabet is a curated 32-character set that excludes visually similar
/// glyphs (<c>0</c>/<c>O</c>, <c>1</c>/<c>I</c>/<c>L</c>) so a code dictated
/// over voice — or read off a phone screen — is unambiguous. The set size
/// is intentionally a power of two so cryptographic-RNG bytes can be
/// modulo-mapped without bias.
/// </para>
/// <para>
/// Codes come from <see cref="RandomNumberGenerator"/> — never
/// <see cref="Random"/> — so they survive being used as low-risk shared
/// secrets (joining a private match is gated by the code, and predictable
/// codes would let a third party grief private lobbies).
/// </para>
/// </remarks>
public static class ShortCodeGenerator
{
    /// <summary>
    /// The 32-character alphabet used to compose codes. Excludes <c>0/O</c>
    /// and <c>1/I/L</c> to reduce voice / handwritten confusion.
    /// </summary>
    public const string Alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

    /// <summary>
    /// Generate a single code of <paramref name="length"/> characters.
    /// </summary>
    /// <param name="length">Code length. Must be ≥ 1. Defaults to 6.</param>
    /// <returns>A fresh random code drawn from <see cref="Alphabet"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> &lt; 1.</exception>
    public static string Generate(int length = 6)
    {
        if (length < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length), length, "Length must be at least 1.");
        }

        // Alphabet.Length == 32; a byte mod 32 is uniform because 256 % 32 == 0.
        // If a future contributor changes the alphabet to a non-power-of-two
        // size they must switch to rejection-sampling — modulo bias kicks in.
        Span<byte> bytes = stackalloc byte[length];
        RandomNumberGenerator.Fill(bytes);

        Span<char> chars = length <= 64 ? stackalloc char[length] : new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = Alphabet[bytes[i] & 0x1F];
        }

        return new string(chars);
    }

    /// <summary>
    /// Generate a code that's not already in use, with up to
    /// <paramref name="maxAttempts"/> collision retries.
    /// </summary>
    /// <param name="length">Code length. Must be ≥ 1.</param>
    /// <param name="isUsed">Async predicate the caller wires to its lookup
    ///   store. Returns <c>true</c> when the code is already taken.</param>
    /// <param name="maxAttempts">Maximum total generation attempts before
    ///   giving up. Must be ≥ 1. Defaults to 8.</param>
    /// <returns>A fresh, unused code.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="isUsed"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> or
    ///   <paramref name="maxAttempts"/> &lt; 1.</exception>
    /// <exception cref="InvalidOperationException">All <paramref name="maxAttempts"/>
    ///   generated codes collided. The exception message names the alphabet size,
    ///   length, and attempt count so an operator can see why the search space
    ///   was exhausted.</exception>
    public static async Task<string> GenerateUniqueAsync(
        int length,
        Func<string, Task<bool>> isUsed,
        int maxAttempts = 8)
    {
        ArgumentNullException.ThrowIfNull(isUsed);

        if (maxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxAttempts), maxAttempts, "maxAttempts must be at least 1.");
        }

        // Generate() validates length ≥ 1.

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            var candidate = Generate(length);
            if (!await isUsed(candidate).ConfigureAwait(false))
            {
                return candidate;
            }
        }

        throw new InvalidOperationException(
            $"ShortCodeGenerator exhausted {maxAttempts} attempts at length {length} " +
            $"against a {Alphabet.Length}-char alphabet without finding an unused code. " +
            "Either the namespace is saturated or `isUsed` is misbehaving.");
    }
}
