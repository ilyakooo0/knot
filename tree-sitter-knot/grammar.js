/// <reference types="tree-sitter-cli/dsl" />
// Tree-sitter grammar for the Knot programming language.
//
// Knot is layout-sensitive. An external scanner emits NEWLINE, INDENT, and
// DEDENT tokens based on indentation. Newlines are NOT in extras — the scanner
// handles all \n characters.

const PREC = {
  PIPE: 1,
  OR: 3,
  AND: 5,
  EQUALITY: 7,
  COMPARISON: 9,
  CONCAT: 11,
  ADD: 13,
  MUL: 15,
  UNARY: 17,
  APP: 18,
  FIELD: 19,
};

module.exports = grammar({
  name: "knot",

  // Spaces, tabs, CR, comments, and line continuations. NO raw newlines.
  extras: ($) => [/[ \t\r]/, $.comment, $._line_cont],

  word: ($) => $.lower_identifier,

  externals: ($) => [$._newline, $._indent, $._dedent, $._line_cont],

  conflicts: ($) => [
    [$.constraint, $.named_type],
    // do-block: pattern vs expression (resolved by presence of `<-`)
    [$.variable_pattern, $.variable_expression],
    [$.record_pattern, $.record_expression],
    [$.list_pattern, $.list_expression],
    [$.literal_pattern, $._atom_expression],
    [$.field_pattern, $.variable_expression],
    // trait param vs type variable in trait declaration
    [$._trait_param, $.type_variable],
    // trait method with default body vs separate default body
    [$.trait_method, $.trait_default_body],
  ],

  rules: {
    // ── Module ──────────────────────────────────────────────────────
    source_file: ($) =>
      seq(
        optional(seq($.module_declaration, $._newline)),
        sep($._declaration, $._newline),
      ),

    module_declaration: ($) =>
      seq("module", field("name", $.upper_identifier)),

    // ── Declarations ────────────────────────────────────────────────
    _declaration: ($) =>
      choice(
        $.import_declaration,
        $.export_declaration,
        $._declaration_body,
      ),

    _declaration_body: ($) =>
      choice(
        $.data_declaration,
        $.type_alias_declaration,
        $.source_declaration,
        $.view_declaration,
        $.derived_declaration,
        $.fun_declaration,
        $.trait_declaration,
        $.impl_declaration,
        $.migrate_declaration,
        $.route_declaration,
        $.route_composite_declaration,
        $.subset_constraint,
      ),

    // ── Import ──────────────────────────────────────────────────────
    import_declaration: ($) =>
      seq(
        "import",
        field("path", $.import_path),
        optional(seq("(", sep1($.import_item, ","), ")")),
      ),

    import_path: ($) =>
      seq(".", "/", sep1($.lower_identifier, "/")),

    import_item: ($) =>
      choice($.lower_identifier, $.upper_identifier),

    // ── Export ───────────────────────────────────────────────────────
    export_declaration: ($) =>
      seq("export", $._declaration_body),

    // ── Data ────────────────────────────────────────────────────────
    data_declaration: ($) =>
      seq(
        "data",
        field("name", $.upper_identifier),
        field("params", repeat($.lower_identifier)),
        choice(
          // Multi-line: indented constructors on separate lines
          seq(
            $._indent,
            sep1($.constructor_def, $._newline),
            optional(seq($._newline, $.deriving_clause)),
            $._dedent,
          ),
          // Single-line: constructors on same line as `data`
          seq(
            repeat1($.constructor_def),
            optional($.deriving_clause),
          ),
        ),
      ),

    constructor_def: ($) =>
      seq(
        choice("=", "|"),
        field("name", $.upper_identifier),
        field("fields", $.record_type_body),
      ),

    deriving_clause: ($) =>
      seq("deriving", "(", sep1($.upper_identifier, ","), ")"),

    // ── Type alias ──────────────────────────────────────────────────
    type_alias_declaration: ($) =>
      seq(
        "type",
        field("name", $.upper_identifier),
        field("params", repeat($.lower_identifier)),
        "=",
        field("type", $._type),
      ),

    // ── Source ───────────────────────────────────────────────────────
    source_declaration: ($) =>
      seq(
        $.source_ref,
        ":",
        field("type", $._type),
        optional(seq("with", "history")),
      ),

    // ── View ────────────────────────────────────────────────────────
    view_declaration: ($) =>
      seq($.source_ref, "=", field("body", $._block_body)),

    // ── Derived ─────────────────────────────────────────────────────
    derived_declaration: ($) =>
      seq($.derived_ref, "=", field("body", $._block_body)),

    // ── Fun ─────────────────────────────────────────────────────────
    fun_declaration: ($) =>
      seq(
        field("name", $.lower_identifier),
        choice(
          seq(
            optional(seq(":", field("type", $.type_scheme))),
            "=",
            field("body", $._block_body),
          ),
          seq(":", field("type", $.type_scheme)),
        ),
      ),

    // ── Trait ────────────────────────────────────────────────────────
    trait_declaration: ($) =>
      seq(
        "trait",
        repeat($.supertrait),
        field("name", $.upper_identifier),
        field("params", repeat1($._trait_param)),
        "where",
        $._indent,
        sep1($._trait_item, $._newline),
        $._dedent,
      ),

    supertrait: ($) =>
      prec.dynamic(1, seq($.upper_identifier, repeat1($._type_atom), "=>")),

    _trait_param: ($) =>
      choice(
        $.lower_identifier,
        seq("(", $.lower_identifier, ":", $._type, ")"),
      ),

    _trait_item: ($) =>
      choice($.trait_method, $.trait_associated_type, $.trait_default_body),

    trait_method: ($) =>
      prec.right(
        seq(
          field("name", $.lower_identifier),
          choice(
            seq(":", field("type", $.type_scheme)),
            seq(repeat($._pattern_atom), "=", $._expression),
          ),
        ),
      ),

    trait_associated_type: ($) =>
      seq("type", $.upper_identifier, repeat($.lower_identifier)),

    trait_default_body: ($) =>
      seq(
        $.lower_identifier,
        repeat($._pattern_atom),
        "=",
        $._expression,
      ),

    // ── Impl ────────────────────────────────────────────────────────
    impl_declaration: ($) =>
      seq(
        "impl",
        repeat(seq($.constraint, "=>")),
        field("trait", $.upper_identifier),
        field("args", repeat1($._type_atom)),
        "where",
        $._indent,
        sep1($._impl_item, $._newline),
        $._dedent,
      ),

    _impl_item: ($) =>
      choice($.impl_method, $.impl_associated_type),

    impl_method: ($) =>
      seq(
        field("name", $.lower_identifier),
        field("params", repeat($._pattern_atom)),
        "=",
        field("body", $._block_body),
      ),

    impl_associated_type: ($) =>
      seq("type", $.upper_identifier, repeat($._type_atom), "=", $._type),

    // ── Migrate ─────────────────────────────────────────────────────
    migrate_declaration: ($) =>
      seq(
        "migrate",
        field("relation", $.source_ref),
        "from",
        field("from", $._type),
        "to",
        field("to", $._type),
        "using",
        field("using", $._expression),
      ),

    // ── Route ───────────────────────────────────────────────────────
    route_declaration: ($) =>
      seq(
        "route",
        field("name", $.upper_identifier),
        "where",
        $._indent,
        sep1($._route_item, $._newline),
        $._dedent,
      ),

    route_composite_declaration: ($) =>
      seq(
        "route",
        field("name", $.upper_identifier),
        "=",
        sep1($.upper_identifier, "|"),
      ),

    _route_item: ($) =>
      choice($.route_entry, $.route_path_group),

    route_path_group: ($) =>
      seq(
        repeat1($._route_path_segment),
        $._indent,
        sep1($._route_item, $._newline),
        $._dedent,
      ),

    route_entry: ($) =>
      prec.right(
        seq(
          field("method", $.http_method),
          repeat(choice(
            $._route_path_segment,
            field("body", $.record_type_body),
            seq("?", field("query", $.record_type_body)),
            seq("headers", $.record_type_body),
          )),
          "->",
          field("response", $._type),
          optional(seq("headers", $.record_type_body)),
          "=",
          field("constructor", $.upper_identifier),
        ),
      ),

    _route_path_segment: ($) =>
      choice(
        seq("/", field("name", $.lower_identifier)),
        seq("/", token.immediate("{"), field("name", $.lower_identifier), ":", field("type", $._type), "}"),
        "/",
      ),

    http_method: ($) => choice("GET", "POST", "PUT", "DELETE", "PATCH"),

    // ── Subset constraints ──────────────────────────────────────────
    subset_constraint: ($) =>
      seq(
        $._constraint_path,
        "<=",
        $._constraint_path,
      ),

    _constraint_path: ($) =>
      choice(
        seq($.source_ref, ".", $.lower_identifier),
        $.source_ref,
      ),

    // ── Types ───────────────────────────────────────────────────────

    type_scheme: ($) => seq(repeat(seq($.constraint, "=>")), $._type),

    constraint: ($) => seq($.upper_identifier, repeat1($._type_atom)),

    _type: ($) => choice($.function_type, $._type_app),

    function_type: ($) =>
      prec.right(
        seq(field("param", $._type_app), "->", field("result", $._type)),
      ),

    _type_app: ($) => choice($.type_application, $._type_atom),

    type_application: ($) =>
      prec.left(
        PREC.APP,
        seq(choice($.named_type, $.type_application), $._type_atom),
      ),

    _type_atom: ($) =>
      choice(
        $.named_type,
        $.type_variable,
        $.record_type,
        $.relation_type,
        $.variant_type,
        $.type_hole,
        $.parenthesized_type,
      ),

    named_type: ($) => $.upper_identifier,
    type_variable: ($) => $.lower_identifier,

    record_type: ($) =>
      seq(
        "{",
        optional(
          choice(
            // Record fields: {name: Type, age: Int}
            seq(
              sep1($.type_field, ","),
              optional(seq("|", $.lower_identifier)),
            ),
            // Bare identifiers for effect sets: {console, network}
            // Also covers: reads *rel, writes *rel
            sep1($.effect, ","),
          ),
        ),
        "}",
      ),

    effect: ($) =>
      choice(
        seq($.lower_identifier, $.source_ref),
        $.lower_identifier,
      ),

    record_type_body: ($) =>
      seq("{", optional(sep1($.type_field, ",")), "}"),

    type_field: ($) =>
      seq(field("name", $.lower_identifier), ":", field("type", $._type)),

    relation_type: ($) => seq("[", optional($._type), "]"),

    variant_type: ($) =>
      seq(
        "<",
        sep1($.variant_constructor, "|"),
        optional(seq("|", $.lower_identifier)),
        ">",
      ),

    variant_constructor: ($) =>
      seq($.upper_identifier, optional($.record_type_body)),

    type_hole: ($) => "_",

    parenthesized_type: ($) => seq("(", $._type, ")"),

    // ── Expressions ─────────────────────────────────────────────────

    // Expression body that may span multiple indented lines.
    // Same-line: just an expression. Multi-line: wrapped in INDENT/DEDENT.
    _block_body: ($) =>
      choice(
        $._expression,
        seq($._indent, $._expression, $._dedent),
      ),

    _expression: ($) =>
      choice(
        $.lambda_expression,
        $.if_expression,
        $.case_expression,
        $.do_expression,
        $.set_expression,
        $.full_set_expression,
        $.yield_expression,
        $.atomic_expression,
        $.let_in_expression,
        $._binary_expression,
      ),

    lambda_expression: ($) =>
      prec.right(
        seq(
          "\\",
          repeat1($._pattern_atom),
          "->",
          field("body", $._expression),
        ),
      ),

    if_expression: ($) =>
      prec.right(
        seq(
          "if",
          field("condition", $._expression),
          "then",
          field("then", $._expression),
          "else",
          field("else", $._expression),
        ),
      ),

    case_expression: ($) =>
      prec.right(
        seq(
          "case",
          field("scrutinee", $._expression),
          "of",
          $._indent,
          sep1($.case_arm, $._newline),
          $._dedent,
        ),
      ),

    case_arm: ($) =>
      prec.right(
        seq(
          field("pattern", $._pattern),
          "->",
          field("body", $._expression),
        ),
      ),

    do_expression: ($) =>
      seq("do", $._indent, sep1($._statement, $._newline), $._dedent),

    _statement: ($) =>
      choice(
        $.bind_statement,
        $.let_statement,
        $.where_statement,
        $._expression,
      ),

    bind_statement: ($) =>
      prec.dynamic(
        1,
        seq(
          field("pattern", $._bind_pattern),
          "<-",
          field("value", $._expression),
        ),
      ),

    _bind_pattern: ($) =>
      choice(
        $.constructor_pattern,
        $.variable_pattern,
        $.wildcard_pattern,
        $.record_pattern,
        $.list_pattern,
        $.parenthesized_pattern,
      ),

    let_statement: ($) =>
      seq(
        "let",
        field("pattern", $._pattern),
        "=",
        field("value", $._expression),
      ),

    where_statement: ($) =>
      prec.right(seq("where", field("condition", $._expression))),

    set_expression: ($) =>
      prec.right(
        seq(
          "set",
          field("target", $.source_ref),
          "=",
          field("value", $._expression),
        ),
      ),

    full_set_expression: ($) =>
      prec.right(
        seq(
          "full",
          "set",
          field("target", $.source_ref),
          "=",
          field("value", $._expression),
        ),
      ),

    yield_expression: ($) =>
      prec.right(seq("yield", field("value", $._expression))),

    atomic_expression: ($) =>
      prec.right(seq("atomic", field("body", $._expression))),

    let_in_expression: ($) =>
      prec.right(
        seq("let", $._pattern, "=", $._expression, "in", $._expression),
      ),

    // ── Binary / unary / application / postfix ──────────────────────

    _binary_expression: ($) =>
      choice(
        $.pipe_expression,
        $.or_expression,
        $.and_expression,
        $.equality_expression,
        $.comparison_expression,
        $.concat_expression,
        $.add_expression,
        $.mul_expression,
        $._unary_expression,
      ),

    pipe_expression: ($) =>
      prec.left(
        PREC.PIPE,
        seq($._binary_expression, "|>", $._binary_expression),
      ),

    or_expression: ($) =>
      prec.left(
        PREC.OR,
        seq($._binary_expression, "||", $._binary_expression),
      ),

    and_expression: ($) =>
      prec.left(
        PREC.AND,
        seq($._binary_expression, "&&", $._binary_expression),
      ),

    equality_expression: ($) =>
      prec.left(
        PREC.EQUALITY,
        seq($._binary_expression, choice("==", "!="), $._binary_expression),
      ),

    comparison_expression: ($) =>
      prec.left(
        PREC.COMPARISON,
        seq(
          $._binary_expression,
          choice("<", ">", "<=", ">="),
          $._binary_expression,
        ),
      ),

    concat_expression: ($) =>
      prec.right(
        PREC.CONCAT,
        seq($._binary_expression, "++", $._binary_expression),
      ),

    add_expression: ($) =>
      prec.left(
        PREC.ADD,
        seq($._binary_expression, choice("+", "-"), $._binary_expression),
      ),

    mul_expression: ($) =>
      prec.left(
        PREC.MUL,
        seq($._binary_expression, choice("*", "/"), $._binary_expression),
      ),

    _unary_expression: ($) =>
      choice(
        $.negation_expression,
        $.not_expression,
        $._application_expression,
      ),

    negation_expression: ($) =>
      prec(PREC.UNARY, seq("-", $._application_expression)),

    not_expression: ($) =>
      prec(PREC.UNARY, seq("not", $._application_expression)),

    _application_expression: ($) =>
      choice($.application_expression, $._postfix_expression),

    application_expression: ($) =>
      prec.left(
        PREC.APP,
        seq($._application_expression, $._postfix_expression),
      ),

    _postfix_expression: ($) =>
      choice($.field_access_expression, $.temporal_expression, $._atom_expression),

    field_access_expression: ($) =>
      prec.left(
        PREC.FIELD,
        seq($._postfix_expression, ".", $.lower_identifier),
      ),

    temporal_expression: ($) =>
      prec.left(
        PREC.FIELD,
        seq($._postfix_expression, "@", "(", $._expression, ")"),
      ),

    _atom_expression: ($) =>
      choice(
        $._literal,
        $.variable_expression,
        $.constructor_expression,
        $.source_ref,
        $.derived_ref,
        $.record_expression,
        $.record_update_expression,
        $.list_expression,
        $.parenthesized_expression,
      ),

    variable_expression: ($) => $.lower_identifier,
    constructor_expression: ($) => $.upper_identifier,

    source_ref: ($) => token(seq("*", /[a-z_][a-zA-Z0-9_']*/)),
    derived_ref: ($) => token(seq("&", /[a-z_][a-zA-Z0-9_']*/)),

    record_expression: ($) =>
      seq("{", optional(sep1($.record_field, ",")), "}"),

    record_field: ($) =>
      choice(
        seq(
          field("name", $.lower_identifier),
          ":",
          field("value", $._expression),
        ),
        field("value", $._expression),
      ),

    record_update_expression: ($) =>
      seq(
        "{",
        field("base", $._expression),
        "|",
        sep1($.update_field, ","),
        "}",
      ),

    update_field: ($) =>
      seq(
        field("name", $.lower_identifier),
        ":",
        field("value", $._expression),
      ),

    list_expression: ($) =>
      seq("[", optional(sep1($._expression, ",")), "]"),

    parenthesized_expression: ($) => seq("(", $._expression, ")"),

    // ── Patterns ────────────────────────────────────────────────────

    _pattern: ($) => choice($.constructor_pattern, $._pattern_atom),

    constructor_pattern: ($) =>
      prec(
        PREC.APP,
        seq(
          field("constructor", $.upper_identifier),
          field("payload", $._pattern_atom),
        ),
      ),

    _pattern_atom: ($) =>
      choice(
        $.variable_pattern,
        $.wildcard_pattern,
        $.literal_pattern,
        $.record_pattern,
        $.list_pattern,
        $.parenthesized_pattern,
      ),

    variable_pattern: ($) => $.lower_identifier,
    wildcard_pattern: ($) => "_",
    literal_pattern: ($) => $._literal,

    record_pattern: ($) =>
      seq("{", optional(sep1($.field_pattern, ",")), "}"),

    field_pattern: ($) =>
      choice(
        seq(
          field("name", $.lower_identifier),
          ":",
          field("pattern", $._pattern),
        ),
        $.lower_identifier,
      ),

    list_pattern: ($) => seq("[", optional(sep1($._pattern, ",")), "]"),

    parenthesized_pattern: ($) => seq("(", $._pattern, ")"),

    // ── Literals ────────────────────────────────────────────────────

    _literal: ($) =>
      choice(
        $.integer_literal,
        $.float_literal,
        $.string_literal,
        $.boolean_literal,
        $.bytes_literal,
      ),

    integer_literal: ($) => /[0-9][0-9_]*/,

    float_literal: ($) => /[0-9][0-9_]*\.[0-9][0-9_]*/,

    string_literal: ($) =>
      seq('"', repeat(choice($.escape_sequence, $.string_content)), '"'),

    boolean_literal: ($) => choice("true", "false"),

    bytes_literal: ($) =>
      seq('b"', repeat(choice($.escape_sequence, $.string_content)), '"'),

    escape_sequence: ($) => token.immediate(prec(1, /\\([\\\"ntr]|x[0-9a-fA-F]{2})/)),

    string_content: ($) => token.immediate(prec(0, /[^"\\]+/)),

    // ── Identifiers ─────────────────────────────────────────────────

    lower_identifier: ($) => /[a-z_][a-zA-Z0-9_']*/,

    upper_identifier: ($) => /[A-Z][a-zA-Z0-9_']*/,

    // ── Misc ────────────────────────────────────────────────────────

    comment: ($) => token(seq("--", /.*/)),
  },
});

function sep1(rule, separator) {
  return seq(rule, repeat(seq(separator, rule)));
}

function sep(rule, separator) {
  return optional(sep1(rule, separator));
}
