using System.Text;
using System.Text.RegularExpressions;

namespace Retcd.Client;

/// <summary>
/// Key patterns. The server only lists by prefix, so the literal text before the first
/// <c>*</c>, <c>?</c> or <c>[</c> goes to the server and the rest is matched here.
/// <c>*</c> = one level (never crosses "/"), <c>**</c> = any depth (zero included),
/// <c>?</c> = one char except "/", <c>[abc]</c> <c>[a-z]</c> <c>[!abc]</c> = one char from a set.
/// </summary>
public static class RetcdGlob
{
    private static readonly char[] GlobChars = { '*', '?', '[' };

    public static bool HasGlob(string pattern) => pattern.IndexOfAny(GlobChars) >= 0;

    /// <summary>The part the server can use as a prefix. The whole text when there is no glob character.</summary>
    public static string LiteralPrefix(string pattern)
    {
        var i = pattern.IndexOfAny(GlobChars);
        return i < 0 ? pattern : pattern[..i];
    }

    /// <summary>Anchored regex for the pattern. Same rules as kv.mjs in the playground.</summary>
    public static Regex ToRegex(string pattern)
    {
        var re = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == '*')
            {
                if (i + 1 < pattern.Length && pattern[i + 1] == '*')
                {
                    while (i + 1 < pattern.Length && pattern[i + 1] == '*') i++;
                    if (i + 1 < pattern.Length && pattern[i + 1] == '/')
                    {
                        i++;
                        re.Append("(?:[\\s\\S]*/)?"); // "**/" also matches no folder at all
                    }
                    else
                    {
                        re.Append("[\\s\\S]*");
                    }
                }
                else
                {
                    re.Append("[^/]*");
                }
            }
            else if (c == '?')
            {
                re.Append("[^/]");
            }
            else if (c == '[')
            {
                var start = i + 1;
                var neg = start < pattern.Length && (pattern[start] == '!' || pattern[start] == '^');
                if (neg) start++;
                // A set needs at least one member, so "[]" and "[!]" stay literal.
                var close = start + 1 <= pattern.Length ? pattern.IndexOf(']', start + 1) : -1;
                if (close < 0)
                {
                    re.Append("\\[");
                }
                else
                {
                    var set = pattern[start..close];
                    re.Append("(?!/)[").Append(neg ? "^" : "");
                    foreach (var ch in set)
                    {
                        if (ch is '\\' or '[' or ']' or '^') re.Append('\\');
                        re.Append(ch);
                    }
                    re.Append(']');
                    i = close;
                }
            }
            else
            {
                re.Append(Regex.Escape(c.ToString()));
            }
        }
        re.Append('$');
        return new Regex(re.ToString(), RegexOptions.CultureInvariant);
    }

    public static bool IsMatch(string pattern, string key) => ToRegex(pattern).IsMatch(key);
}
