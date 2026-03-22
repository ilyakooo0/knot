; Code folding regions.

[
  (do_expression)
  (case_expression)
  (data_declaration)
  (trait_declaration)
  (impl_declaration)
  (migrate_declaration)
  (route_declaration)
  (if_expression)
  (lambda_expression)
] @fold

; Bracketed constructs
[
  (record_expression)
  (record_update_expression)
  (record_type)
  (list_expression)
  (variant_type)
] @fold

; Import groups (fold multiple imports)
(import_declaration) @fold
