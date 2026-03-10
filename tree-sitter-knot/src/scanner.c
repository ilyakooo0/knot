// External scanner for Knot's layout-sensitive syntax.
//
// Emits layout tokens based on indentation:
// - NEWLINE: separates statements/declarations at the same indent level
// - INDENT: marks the start of a more-indented block
// - DEDENT: marks the end of an indented block
// - LINE_CONT: a continuation newline (in extras, consumed as whitespace)
//
// Newlines (\n) are NOT in extras — this scanner handles all of them.

#include "tree_sitter/parser.h"

#include <stdbool.h>
#include <string.h>

enum TokenType {
  LAYOUT_NEWLINE,
  LAYOUT_INDENT,
  LAYOUT_DEDENT,
  LINE_CONT,
};

#define MAX_DEPTH 64

typedef struct {
  uint16_t stack[MAX_DEPTH];
  uint8_t depth;
  uint8_t pending_dedents;
  bool pending_newline;
} Scanner;

static uint16_t top(Scanner *s) { return s->stack[s->depth]; }

void *tree_sitter_knot_external_scanner_create(void) {
  Scanner *s = calloc(1, sizeof(Scanner));
  s->depth = 0;
  s->stack[0] = 0;
  s->pending_dedents = 0;
  s->pending_newline = false;
  return s;
}

void tree_sitter_knot_external_scanner_destroy(void *payload) {
  free(payload);
}

unsigned tree_sitter_knot_external_scanner_serialize(void *payload,
                                                     char *buffer) {
  Scanner *s = (Scanner *)payload;
  unsigned size = 0;

  buffer[size++] = (char)s->depth;
  buffer[size++] = (char)s->pending_dedents;
  buffer[size++] = (char)s->pending_newline;
  for (uint8_t i = 0; i <= s->depth; i++) {
    if (size + 2 > TREE_SITTER_SERIALIZATION_BUFFER_SIZE) break;
    buffer[size++] = (char)(s->stack[i] & 0xFF);
    buffer[size++] = (char)((s->stack[i] >> 8) & 0xFF);
  }

  return size;
}

void tree_sitter_knot_external_scanner_deserialize(void *payload,
                                                    const char *buffer,
                                                    unsigned length) {
  Scanner *s = (Scanner *)payload;
  s->depth = 0;
  s->stack[0] = 0;
  s->pending_dedents = 0;
  s->pending_newline = false;

  if (length == 0) return;

  unsigned pos = 0;
  s->depth = (uint8_t)buffer[pos++];
  if (pos < length) s->pending_dedents = (uint8_t)buffer[pos++];
  if (pos < length) s->pending_newline = (bool)buffer[pos++];

  for (uint8_t i = 0; i <= s->depth && pos + 1 < length; i++) {
    s->stack[i] =
        (uint16_t)((uint8_t)buffer[pos]) |
        (uint16_t)((uint8_t)buffer[pos + 1] << 8);
    pos += 2;
  }
}

bool tree_sitter_knot_external_scanner_scan(void *payload, TSLexer *lexer,
                                             const bool *valid_symbols) {
  Scanner *s = (Scanner *)payload;

  // Handle queued dedents first (zero-width)
  if (s->pending_dedents > 0 && valid_symbols[LAYOUT_DEDENT]) {
    s->pending_dedents--;
    if (s->depth > 0) s->depth--;
    lexer->result_symbol = LAYOUT_DEDENT;
    return true;
  }

  // After all dedents emitted, emit a NEWLINE to separate items at the same level
  if (s->pending_newline) {
    s->pending_newline = false;
    // At EOF, no separator needed — the file is done
    if (lexer->eof(lexer)) {
      return false;
    }
    if (valid_symbols[LAYOUT_NEWLINE]) {
      lexer->result_symbol = LAYOUT_NEWLINE;
      return true;
    }
    // If NEWLINE not valid, treat as continuation
    lexer->result_symbol = LINE_CONT;
    return true;
  }

  // Skip spaces/tabs/CR (not newlines)
  while (lexer->lookahead == ' ' || lexer->lookahead == '\t' ||
         lexer->lookahead == '\r') {
    lexer->advance(lexer, true);
  }

  // Handle EOF — close any open blocks
  if (lexer->eof(lexer)) {
    if (valid_symbols[LAYOUT_NEWLINE] && s->depth > 0) {
      lexer->result_symbol = LAYOUT_NEWLINE;
      return true;
    }
    if (valid_symbols[LAYOUT_DEDENT] && s->depth > 0) {
      s->depth--;
      lexer->result_symbol = LAYOUT_DEDENT;
      return true;
    }
    return false;
  }

  // We need to see a newline character
  if (lexer->lookahead != '\n') {
    return false;
  }

  // Consume newline
  lexer->advance(lexer, true);

  // Skip blank lines and measure indentation of next content line
  uint16_t indent = 0;
  for (;;) {
    indent = 0;
    while (lexer->lookahead == ' ' || lexer->lookahead == '\t' ||
           lexer->lookahead == '\r') {
      if (lexer->lookahead == '\t')
        indent += 2;
      else if (lexer->lookahead == ' ')
        indent++;
      lexer->advance(lexer, true);
    }

    if (lexer->eof(lexer)) {
      indent = 0;
      break;
    }

    if (lexer->lookahead == '\n') {
      // Blank line — skip
      lexer->advance(lexer, true);
      continue;
    }

    break;
  }

  lexer->mark_end(lexer);

  uint16_t current = top(s);

  // Dedent: indent decreased — end block(s)
  if (indent < current) {
    // Count how many levels to pop
    uint8_t levels = 0;
    uint8_t d = s->depth;
    while (d > 0 && s->stack[d] > indent) {
      levels++;
      d--;
    }
    s->pending_dedents = levels;
    s->pending_newline = true;

    // Emit first dedent
    if (s->pending_dedents > 0 && valid_symbols[LAYOUT_DEDENT]) {
      s->pending_dedents--;
      s->depth--;
      lexer->result_symbol = LAYOUT_DEDENT;
      return true;
    }

    // If DEDENT not valid, try NEWLINE
    if (valid_symbols[LAYOUT_NEWLINE]) {
      s->pending_dedents = 0;
      s->pending_newline = false;
      lexer->result_symbol = LAYOUT_NEWLINE;
      return true;
    }

    // Fallback to line continuation
    s->pending_dedents = 0;
    s->pending_newline = false;
    lexer->result_symbol = LINE_CONT;
    return true;
  }

  // Same indent: statement/declaration separator
  if (indent == current) {
    if (valid_symbols[LAYOUT_NEWLINE]) {
      lexer->result_symbol = LAYOUT_NEWLINE;
      return true;
    }

    // If NEWLINE not valid here, treat as continuation
    lexer->result_symbol = LINE_CONT;
    return true;
  }

  // Indent increased: new block
  if (valid_symbols[LAYOUT_INDENT]) {
    if (s->depth < MAX_DEPTH - 1) {
      s->depth++;
      s->stack[s->depth] = indent;
    }
    lexer->result_symbol = LAYOUT_INDENT;
    return true;
  }

  // INDENT not valid — treat as continuation (e.g., lambda body on next line)
  lexer->result_symbol = LINE_CONT;
  return true;
}
