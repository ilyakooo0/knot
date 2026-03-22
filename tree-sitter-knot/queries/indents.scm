; Auto-indentation hints for editors.
; Knot is layout-sensitive — indentation defines block structure.

; ── Indent ────────────────────────────────────────────────

; Block-starting constructs increase indentation
[
  (do_expression)
  (case_expression)
  (trait_declaration)
  (impl_declaration)
  (data_declaration)
  (route_declaration)
  (route_path_group)
] @indent

; Bracketed constructs
[
  (record_expression)
  (record_update_expression)
  (record_type)
  (record_type_body)
  (list_expression)
  (parenthesized_expression)
  (variant_type)
] @indent

; ── Outdent ───────────────────────────────────────────────

; Closing brackets reduce indentation
[
  "}"
  "]"
  ")"
  ">"
] @outdent
