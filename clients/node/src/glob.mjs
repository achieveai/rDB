// Patterns. The server only knows prefixes, so the literal part of a pattern goes to the
// server and the rest is matched here. Same rules as samples/retcd-playground/kv.mjs.
//   *  within one segment (never crosses /)     **  any depth, zero included
//   ?  one char except /                        [abc] [a-z] [!abc]  one char from a set

const GLOB_CHARS = /[*?[]/;

/** True if the text has a glob character (* ? [). */
export const hasGlob = (p) => GLOB_CHARS.test(p);

/** The text before the first glob character: what the server is asked to scan. */
export const literalPrefix = (p) => (hasGlob(p) ? p.slice(0, p.search(GLOB_CHARS)) : p);

/** Compile a glob to an anchored RegExp. */
export function globToRegExp(pattern) {
  let re = '';
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i];
    if (c === '*') {
      if (pattern[i + 1] === '*') {
        while (pattern[i + 1] === '*') i++;
        if (pattern[i + 1] === '/') {
          i++;
          re += '(?:[\\s\\S]*/)?'; // "**/" also matches no directory at all
        } else re += '[\\s\\S]*';
      } else re += '[^/]*';
    } else if (c === '?') re += '[^/]';
    else if (c === '[') {
      const close = pattern.indexOf(']', i + 2); // "[]" and "[!]" are not sets: a literal [
      if (close === -1) re += '\\[';
      else {
        let set = pattern.slice(i + 1, close);
        const neg = set[0] === '!' || set[0] === '^';
        if (neg) set = set.slice(1);
        re += `(?!/)[${neg ? '^' : ''}${set.replace(/[\\[\]^]/g, '\\$&')}]`;
        i = close;
      }
    } else re += c.replace(/[.+^${}()|\\\]]/g, '\\$&');
  }
  return new RegExp(`^${re}$`);
}
