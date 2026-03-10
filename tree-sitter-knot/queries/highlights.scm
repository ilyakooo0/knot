; Keywords
[
  "module"
  "data"
  "type"
  "trait"
  "impl"
  "migrate"
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
  "full"
  "atomic"
  "deriving"
  "with"
  "from"
  "to"
  "using"
  "history"
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
] @operator

; Punctuation
["(" ")" "{" "}" "[" "]"] @punctuation.bracket
["," "." ":" "*"] @punctuation.delimiter

; Literals
(integer_literal) @number
(float_literal) @number.float
(string_literal) @string
(escape_sequence) @string.escape

; Comments
(comment) @comment.line

; Identifiers
(upper_identifier) @type
(constructor_expression) @constructor
(constructor_pattern (upper_identifier) @constructor)
(constructor_def (upper_identifier) @constructor)

; Type declarations
(data_declaration name: (upper_identifier) @type.definition)
(type_alias_declaration name: (upper_identifier) @type.definition)
(trait_declaration name: (upper_identifier) @type.definition)

; Function definitions
(fun_declaration name: (lower_identifier) @function)
(impl_item name: (lower_identifier) @function)
(trait_item name: (lower_identifier) @function)

; Variables
(variable_expression) @variable
(variable_pattern) @variable

; Source and derived references
(source_ref) @variable.builtin
(derived_ref) @variable.builtin

; Field access
(field_access_expression "." (lower_identifier) @property)
(record_field name: (lower_identifier) @property)
(update_field name: (lower_identifier) @property)
(type_field name: (lower_identifier) @property)
(field_pattern name: (lower_identifier) @property)

; Module
(module_declaration (upper_identifier) @module)

; Wildcards
(wildcard_pattern) @variable.builtin
