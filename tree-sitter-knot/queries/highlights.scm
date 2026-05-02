; Keywords
[
  "module"
  "data"
  "type"
  "trait"
  "impl"
  "migrate"
  "route"
  "import"
  "export"
  "where"
  "do"
  "yield"
  "set"
  "if"
  "then"
  "else"
  "case"
  "of"
  "let"
  "in"
  "not"
  "replace"
  "atomic"
  "refine"
  "deriving"
  "with"
  "from"
  "to"
  "using"
  "history"
  "headers"
] @keyword

; Operators
[
  "+"
  "-"
  "*"
  "/"
  "++"
  "=="
  "!="
  "<"
  ">"
  "<="
  ">="
  "&&"
  "||"
  "|>"
  "->"
  "<-"
  "=>"
  "="
  "|"
  "\\"
  "@"
] @operator

; HTTP methods in routes
(http_method) @keyword.directive

; Punctuation
["(" ")" "{" "}" "[" "]"] @punctuation.bracket
["," "." ":" "/"] @punctuation.delimiter

; Literals
(integer_literal) @number
(float_literal) @number.float
(string_literal) @string
(bytes_literal) @string
(escape_sequence) @string.escape
(boolean_literal) @boolean

; Comments
(comment) @comment.line

; ── Type names ──────────────────────────────────────────────────

; Type declarations
(data_declaration name: (upper_identifier) @type.definition)
(type_alias_declaration name: (upper_identifier) @type.definition)
(trait_declaration name: (upper_identifier) @type.definition)

; Constructors
(constructor_expression) @constructor
(constructor_pattern (upper_identifier) @constructor)
(constructor_def (upper_identifier) @constructor)

; Named types in type positions
(named_type) @type

; Variant type constructors
(variant_constructor (upper_identifier) @constructor)

; Supertrait constraint type names
(supertrait (upper_identifier) @type)

; Constraint type names
(constraint (upper_identifier) @type)

; Impl trait name
(impl_declaration trait: (upper_identifier) @type)

; ── Function definitions ────────────────────────────────────────

(fun_declaration name: (lower_identifier) @function)
(impl_method name: (lower_identifier) @function)
(trait_method name: (lower_identifier) @function)
(trait_default_body (lower_identifier) @function)

; ── Variables ───────────────────────────────────────────────────

(variable_expression) @variable
(variable_pattern) @variable

; ── Source and derived references ───────────────────────────────

(source_ref) @variable.builtin
(derived_ref) @variable.builtin

; ── Field access ────────────────────────────────────────────────

(field_access_expression "." (lower_identifier) @property)
(record_field name: (lower_identifier) @property)
(update_field name: (lower_identifier) @property)
(type_field name: (lower_identifier) @property)
(field_pattern name: (lower_identifier) @property)

; ── Effects in type annotations ─────────────────────────────────

; Highlight effect keywords like reads, writes, console, etc.
(effect
  (lower_identifier) @type.qualifier)
(effect
  (lower_identifier) @type.qualifier
  (source_ref))

; ── Route entries ───────────────────────────────────────────────

; Route path parameter names
(route_entry constructor: (upper_identifier) @constructor)

; ── Associated types ────────────────────────────────────────────

(trait_associated_type (upper_identifier) @type.definition)
(impl_associated_type (upper_identifier) @type.definition)

; ── Import ──────────────────────────────────────────────────────

(import_path) @string.special
(import_item (upper_identifier) @type)
(import_item (lower_identifier) @function)

; ── Module ──────────────────────────────────────────────────────

(module_declaration (upper_identifier) @module)

; ── Wildcards ───────────────────────────────────────────────────

(wildcard_pattern) @variable.builtin
(type_hole) @type

; ── Builtin functions (highlighted via predicate) ───────────────

; IO and concurrency builtins
((variable_expression) @function.builtin
  (#any-of? @function.builtin
    "println" "print" "show" "readLine"
    "readFile" "writeFile" "appendFile" "fileExists" "removeFile" "listDir"
    "now" "fork" "retry" "listen"
    "fetch" "fetchWith"
    "randomInt" "randomFloat"
    "toJson" "parseJson"))

; Collection builtins
((variable_expression) @function.builtin
  (#any-of? @function.builtin
    "union" "count" "filter" "match" "map" "fold" "single"
    "diff" "inter" "sum" "avg" "take" "drop" "length"
    "toUpper" "toLower" "trim" "contains" "reverse" "chars"
    "id" "not"))

; groupBy — context-sensitive keyword in do-blocks
((variable_expression) @keyword
  (#eq? @keyword "groupBy"))

; IO as a builtin type
((named_type) @type.builtin
  (#eq? @type.builtin "IO"))

; Common ADT types
((named_type) @type.builtin
  (#any-of? @type.builtin "Maybe" "Result" "Bool" "Int" "Float" "Text" "Bytes"))
