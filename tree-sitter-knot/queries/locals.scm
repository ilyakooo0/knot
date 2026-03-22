; Scope-aware variable resolution for syntax highlighting.
; Defines scopes, definitions, and references so editors can
; distinguish shadowed bindings and resolve names locally.

; ── Scopes ────────────────────────────────────────────────

(source_file) @local.scope

(fun_declaration
  body: (_) @local.scope)

(lambda_expression) @local.scope

(do_expression) @local.scope

(case_arm) @local.scope

(let_in_expression) @local.scope

(impl_method) @local.scope

(trait_default_body) @local.scope

; ── Definitions ───────────────────────────────────────────

; Top-level functions
(fun_declaration
  name: (lower_identifier) @local.definition)

; Variables introduced by patterns (bind, let, case, lambda params)
(variable_pattern) @local.definition

; Record field punning in patterns (acts as a binding)
(field_pattern
  name: (lower_identifier) @local.definition)

; Type definitions
(data_declaration
  name: (upper_identifier) @local.definition)

(type_alias_declaration
  name: (upper_identifier) @local.definition)

; Constructors
(constructor_def
  name: (upper_identifier) @local.definition)

; Trait and impl method definitions
(trait_declaration
  name: (upper_identifier) @local.definition)

(trait_method
  name: (lower_identifier) @local.definition)

(impl_method
  name: (lower_identifier) @local.definition)

; Import items
(import_item
  (upper_identifier) @local.definition)
(import_item
  (lower_identifier) @local.definition)

; ── References ────────────────────────────────────────────

(variable_expression) @local.reference

(constructor_expression) @local.reference
