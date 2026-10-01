using Retcd.Client;

namespace Retcd.Client.Tests;

public class GlobTests
{
    [Theory]
    [InlineData("docs/", "docs/", false)]
    [InlineData("docs/*", "docs/", true)]
    [InlineData("docs/**/*.md", "docs/", true)]
    [InlineData("a?b", "a", true)]
    [InlineData("[ab]x", "", true)]
    [InlineData("*", "", true)]
    [InlineData("", "", false)]
    public void Literal_prefix_stops_at_first_glob_character(string pattern, string prefix, bool hasGlob)
    {
        Assert.Equal(prefix, RetcdGlob.LiteralPrefix(pattern));
        Assert.Equal(hasGlob, RetcdGlob.HasGlob(pattern));
    }

    [Theory]
    // * is one level
    [InlineData("docs/*", "docs/a.md", true)]
    [InlineData("docs/*", "docs/sub/a.md", false)]
    [InlineData("docs/*", "docs/", true)]
    [InlineData("docs/*.md", "docs/a.md", true)]
    [InlineData("docs/*.md", "docs/a.mdx", false)]
    // ** is any depth, zero included
    [InlineData("docs/**/*.md", "docs/a.md", true)]
    [InlineData("docs/**/*.md", "docs/x/y/a.md", true)]
    [InlineData("docs/**/*.md", "docs/x/y/a.txt", false)]
    [InlineData("docs/**", "docs/x/y", true)]
    [InlineData("**/a", "a", true)]
    [InlineData("**/a", "x/y/a", true)]
    // ? is one char, not "/"
    [InlineData("a?c", "abc", true)]
    [InlineData("a?c", "a/c", false)]
    [InlineData("a?c", "ac", false)]
    // sets
    [InlineData("f[a-c]", "fb", true)]
    [InlineData("f[a-c]", "fd", false)]
    [InlineData("f[!a-c]", "fd", true)]
    [InlineData("f[!a-c]", "fb", false)]
    [InlineData("f[abc]", "f/", false)]
    // regex characters in the pattern are literal
    [InlineData("a.b", "a.b", true)]
    [InlineData("a.b", "aXb", false)]
    [InlineData("a+b(1)", "a+b(1)", true)]
    // a "[" with no closing "]", or an empty set, is a literal
    [InlineData("a[b", "a[b", true)]
    [InlineData("a[]b", "a[]b", true)]
    [InlineData("a[!]b", "a[!]b", true)]
    // anchored on both ends
    [InlineData("docs", "docs/a", false)]
    public void Pattern_matches_like_kv_mjs(string pattern, string key, bool expected)
    {
        Assert.Equal(expected, RetcdGlob.IsMatch(pattern, key));
    }
}
