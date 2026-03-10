; Code navigation tags (definitions and references).
; Used by editors for go-to-definition, symbol search, etc.

; ── Definitions ───────────────────────────────────────────

; Functions
(fun_declaration
  name: (lower_identifier) @name) @definition.function

; Data types
(data_declaration
  name: (upper_identifier) @name) @definition.type

; Type aliases
(type_alias_declaration
  name: (upper_identifier) @name) @definition.type

; Constructors
(constructor_def
  name: (upper_identifier) @name) @definition.constructor

; Traits
(trait_declaration
  name: (upper_identifier) @name) @definition.interface

; Trait methods
(trait_item
  name: (lower_identifier) @name) @definition.method

; Impl blocks
(impl_declaration
  trait: (upper_identifier) @name) @definition.implementation

; Impl methods
(impl_item
  name: (lower_identifier) @name) @definition.method

; Source relations
(source_declaration
  (source_ref) @name) @definition.variable

; Views
(view_declaration
  (source_ref) @name) @definition.variable

; Derived relations
(derived_declaration
  (derived_ref) @name) @definition.variable

; ── References ────────────────────────────────────────────

; Function calls (application of a variable)
(application_expression
  .
  (variable_expression) @name) @reference.call

; Constructor usage
(constructor_expression) @name @reference.class
