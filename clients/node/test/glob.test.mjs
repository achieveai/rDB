// Pure tests: pattern matching and prefix extraction. No cluster.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { globToRegExp, hasGlob, literalPrefix } from '../src/glob.mjs';

const m = (pattern, key) => globToRegExp(pattern).test(key);

test('literalPrefix is the text before the first glob character', () => {
  assert.equal(literalPrefix('docs/*.md'), 'docs/');
  assert.equal(literalPrefix('docs/**/a'), 'docs/');
  assert.equal(literalPrefix('a/b?c'), 'a/b');
  assert.equal(literalPrefix('a/b[0-9]'), 'a/b');
  assert.equal(literalPrefix('*'), '');
  assert.equal(literalPrefix('plain/prefix/'), 'plain/prefix/');
  assert.equal(literalPrefix(''), '');
});

test('hasGlob only fires on * ? [', () => {
  assert.equal(hasGlob('a/b'), false);
  assert.equal(hasGlob('a.b+c(d)'), false);
  for (const p of ['a*', 'a?', 'a[b]']) assert.equal(hasGlob(p), true, p);
});

test('* stays inside one segment', () => {
  assert.equal(m('docs/*', 'docs/a'), true);
  assert.equal(m('docs/*', 'docs/a.md'), true);
  assert.equal(m('docs/*', 'docs/a/b'), false);
  assert.equal(m('docs/*', 'other/a'), false);
  assert.equal(m('docs/*.md', 'docs/a.md'), true);
  assert.equal(m('docs/*.md', 'docs/a.txt'), false);
});

test('** crosses folders, zero included', () => {
  assert.equal(m('docs/**', 'docs/a/b/c'), true);
  assert.equal(m('docs/**/*.md', 'docs/a.md'), true); // "**/" may match no folder
  assert.equal(m('docs/**/*.md', 'docs/x/y/a.md'), true);
  assert.equal(m('docs/**/*.md', 'docs/x/y/a.txt'), false);
  assert.equal(m('**/a', 'a'), true);
  assert.equal(m('**/a', 'x/y/a'), true);
});

test('? is one character, never a slash', () => {
  assert.equal(m('a?c', 'abc'), true);
  assert.equal(m('a?c', 'ac'), false);
  assert.equal(m('a?c', 'a/c'), false);
});

test('[..] sets, ranges and negation', () => {
  assert.equal(m('[a-c]x', 'bx'), true);
  assert.equal(m('[a-c]x', 'dx'), false);
  assert.equal(m('[!a-c]x', 'dx'), true);
  assert.equal(m('[!a-c]x', 'bx'), false);
  assert.equal(m('[^a-c]x', 'dx'), true);
  assert.equal(m('[!a]', '/'), false); // a negated set never matches a slash
});

test('an unclosed [ and regex characters are literal', () => {
  assert.equal(m('a[b', 'a[b'), true);
  assert.equal(m('a.b', 'a.b'), true);
  assert.equal(m('a.b', 'axb'), false);
  assert.equal(m('a+b(1)|c', 'a+b(1)|c'), true);
  assert.equal(m('a{1}$', 'a{1}$'), true);
});

test('the match is anchored at both ends', () => {
  assert.equal(m('a*', 'xa'), false);
  assert.equal(m('*a', 'ab'), false);
});
