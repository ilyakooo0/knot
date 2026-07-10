//! Minimal type resolution for schema generation.
//!
//! Resolves type aliases and computes schema descriptors that the
//! runtime uses to create SQLite tables and read/write rows.

use knot::ast::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ResolvedType {
    Int,
    Float,
    Text,
    Bool,
    Bytes,
    Uuid,
    Unit,
    Record(Vec<(String, ResolvedType)>),
    Relation(Box<ResolvedType>),
    Function(Box<ResolvedType>, Box<ResolvedType>),
    Named(String),
    /// Multi-variant ADT: Vec<(constructor_name, constructor_fields)>
    Adt(Vec<(String, Vec<(String, ResolvedType)>)>),
}

/// An associated type definition from an impl block.
/// e.g. `type Item [a] = a` produces args=[Relation(Var("a"))], ty=Var("a")
#[derive(Debug, Clone)]
pub struct AssocTypeDef {
    pub args: Vec<Type>,
    pub ty: Type,
}

/// Parameterized single-variant data type info: name -> (param names, raw
/// field types). Used to substitute type arguments at application sites
/// (e.g. `Box Int`) so the schema reflects the actual arg rather than the
/// placeholder used during alias pre-resolution.
pub struct TypeEnv {
    #[allow(dead_code)]
    pub aliases: HashMap<String, ResolvedType>,
    /// constructor_name -> Vec<(field_name, field_type)>
    pub constructors: HashMap<String, Vec<(String, ResolvedType)>>,
    /// source_name -> schema descriptor string ("col:type,col:type,...")
    pub source_schemas: HashMap<String, String>,
    /// relation_name -> Vec<(old_schema, new_schema)> from `migrate` declarations
    pub migrate_schemas: HashMap<String, Vec<(String, String)>>,
    /// Associated type definitions: assoc_type_name -> definitions from impls
    #[allow(dead_code)]
    pub associated_types: HashMap<String, Vec<AssocTypeDef>>,
    /// Subset constraints: (sub, sup)
    pub subset_constraints: Vec<(RelationPath, RelationPath)>,
    /// Source refined field info: source_name -> [(field_name_or_none, refined_type_name, predicate_expr)]
    /// None field_name means the whole element type is refined.
    pub source_refinements: HashMap<String, Vec<(Option<String>, String, Expr)>>,
    /// Refined type aliases: type_name -> predicate expr
    pub refined_types: HashMap<String, Expr>,
}

impl TypeEnv {
    #[allow(clippy::type_complexity)]
    pub fn from_module(module: &Module) -> Self {
        let mut aliases = HashMap::new();
        let mut constructors = HashMap::new();
        let mut source_schemas = HashMap::new();
        let mut migrate_schemas: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut associated_types: HashMap<String, Vec<AssocTypeDef>> = HashMap::new();

        let mut refined_types: HashMap<String, Expr> = HashMap::new();
        // Original AST types for aliases — used to resolve refinements through aliases
        let mut alias_ast_types: HashMap<String, Type> = HashMap::new();
        // Multi-variant data declarations — used to collect constructor
        // field refinements for direct-ADT sources (`*shapes : [Shape]`).
        let mut data_ctor_decls: HashMap<String, Vec<ConstructorDef>> = HashMap::new();
        // Parameterized single-variant data types: name -> (param names,
        // raw field types). Used to substitute type arguments at application
        // sites (e.g. `Box Int`) so the schema reflects the actual arg
        // rather than the placeholder used during alias pre-resolution.
        let mut single_variant_params: HashMap<String, (Vec<String>, Vec<(String, Type)>)> =
            HashMap::new();

        // First pass: collect type aliases and data types
        for decl in &module.decls {
            match &decl.node {
                DeclKind::TypeAlias { name, params, ty }
                    if params.is_empty() => {
                        alias_ast_types.insert(name.clone(), ty.clone());
                        // Track refined type aliases separately
                        if let TypeKind::Refined { base, predicate } = &ty.node {
                            refined_types.insert(name.clone(), (**predicate).clone());
                            let resolved =
                                resolve_type(base, &aliases, &associated_types, &single_variant_params);
                            aliases.insert(name.clone(), resolved);
                        } else {
                            let resolved =
                                resolve_type(ty, &aliases, &associated_types, &single_variant_params);
                            aliases.insert(name.clone(), resolved);
                        }
                    }
                DeclKind::Data {
                    name,
                    params,
                    constructors: ctors,
                    ..
                } => {
                    // For single-variant data types, treat as a record alias
                    if ctors.len() == 1 {
                        let ctor = &ctors[0];
                        let fields: Vec<(String, ResolvedType)> = ctor
                            .fields
                            .iter()
                            .map(|f| {
                                (
                                    f.name.clone(),
                                    resolve_type(
                                        &f.value,
                                        &aliases,
                                        &associated_types,
                                        &single_variant_params,
                                    ),
                                )
                            })
                            .collect();
                        aliases.insert(
                            name.clone(),
                            ResolvedType::Record(fields.clone()),
                        );
                        // For parameterized single-variant data types
                        // (`data Box a = Box {value: a}`), capture the type
                        // parameters and the raw field-type AST so an
                        // application like `Box Int` can substitute the arg
                        // into the field types at use site (otherwise the
                        // resolved alias collapses every type parameter to
                        // `Named("unknown")`, producing wrong column types).
                        if !params.is_empty() {
                            single_variant_params.insert(
                                name.clone(),
                                (
                                    params.clone(),
                                    ctor.fields
                                        .iter()
                                        .map(|f| (f.name.clone(), f.value.clone()))
                                        .collect(),
                                ),
                            );
                        }
                        // Register the AST record view under the data-type name
                        // too, so a source of this type (`*account : [Money]`)
                        // has its field-level refinements collected via the
                        // alias arm of `collect_source_refinements` — otherwise
                        // single-variant data field refinements are silently
                        // dropped (multi-variant ones go through data_ctor_decls).
                        alias_ast_types.entry(name.clone()).or_insert_with(|| {
                            Spanned::new(
                                TypeKind::Record {
                                    fields: ctor.fields.clone(),
                                    rest: None,
                                },
                                decl.span,
                            )
                        });
                        // First-wins: a duplicate ctor name across data types
                        // is reported as an error in inference; here we just
                        // need a consistent view of "the" Circle.
                        constructors.entry(ctor.name.clone()).or_insert(fields);
                    } else {
                        // Multi-variant: register each constructor and create Adt alias
                        data_ctor_decls.insert(name.clone(), ctors.clone());
                        let mut adt_ctors = Vec::new();
                        for ctor in ctors {
                            let fields: Vec<(String, ResolvedType)> = ctor
                                .fields
                                .iter()
                                .map(|f| {
                                    (
                                        f.name.clone(),
                                        resolve_type(
                                            &f.value,
                                            &aliases,
                                            &associated_types,
                                            &single_variant_params,
                                        ),
                                    )
                                })
                                .collect();
                            constructors
                                .entry(ctor.name.clone())
                                .or_insert_with(|| fields.clone());
                            adt_ctors.push((ctor.name.clone(), fields));
                        }
                        aliases.insert(name.clone(), ResolvedType::Adt(adt_ctors));
                    }
                }
                DeclKind::Impl { items, .. } => {
                    for item in items {
                        if let ImplItem::AssociatedType { name, args, ty } =
                            item
                        {
                            associated_types
                                .entry(name.clone())
                                .or_default()
                                .push(AssocTypeDef {
                                    args: args.clone(),
                                    ty: ty.clone(),
                                });
                        }
                    }
                }
                _ => {}
            }
        }

        // Re-resolve pass: fix forward references in type aliases.
        // After the first pass, some aliases may contain Named("X") where X
        // was defined later. Re-resolve those now that all aliases are known.
        // A single pass suffices: `re_resolve_inner` recurses into each
        // replacement (with a seen-set for cycle protection), so chained
        // forward refs (A→B→C) resolve transitively regardless of HashMap
        // iteration order. Do NOT loop until stable — a recursive type
        // (`type A = {x: A}`, mutual cycles, self-referential data) makes
        // every pass expand the structure one more level, so a
        // run-until-stable loop grows it without bound until the stack
        // overflows. Cyclic aliases are reported as diagnostics by
        // `check_alias_cycles` / type inference before codegen runs.
        let alias_keys: Vec<String> = aliases.keys().cloned().collect();
        for name in &alias_keys {
            let resolved = aliases[name].clone();
            let fixed = re_resolve_type(&resolved, &aliases);
            if fixed != resolved {
                aliases.insert(name.clone(), fixed);
            }
        }
        // Also re-resolve constructor fields
        let ctor_keys: Vec<String> = constructors.keys().cloned().collect();
        for name in &ctor_keys {
            let fields = constructors[name].clone();
            let fixed: Vec<(String, ResolvedType)> = fields
                .into_iter()
                .map(|(n, t)| (n, re_resolve_type(&t, &aliases)))
                .collect();
            constructors.insert(name.clone(), fixed);
        }

        // Second pass: compute source schemas, migration schemas, and subset constraints
        let mut subset_constraints = Vec::new();
        let mut source_refinements: HashMap<String, Vec<(Option<String>, String, Expr)>> = HashMap::new();
        for decl in &module.decls {
            match &decl.node {
                DeclKind::Source { name, ty } => {
                    let schema =
                        schema_for_source(ty, &aliases, &associated_types, &single_variant_params);
                    source_schemas.insert(name.clone(), schema);
                    // Collect refined field info from the source type
                    let refinements = collect_source_refinements(ty, &refined_types, &alias_ast_types, &data_ctor_decls);
                    if !refinements.is_empty() {
                        source_refinements.insert(name.clone(), refinements);
                    }
                }
                DeclKind::Migrate {
                    relation,
                    from_ty,
                    to_ty,
                    ..
                } => {
                    // Unwrap relation types (`[{...}]`, or aliases that
                    // resolve to one) the same way `schema_for_source`
                    // does — `schema_descriptor` on a `Relation` collapses
                    // to a bare "text", which breaks every startup.
                    let unwrap_relation = |r: ResolvedType| match r {
                        ResolvedType::Relation(inner) => *inner,
                        other => other,
                    };
                    let old_resolved = unwrap_relation(
                        resolve_type(from_ty, &aliases, &associated_types, &single_variant_params),
                    );
                    let new_resolved = unwrap_relation(
                        resolve_type(to_ty, &aliases, &associated_types, &single_variant_params),
                    );
                    // Use `relation_inner_schema` (not bare `schema_descriptor`)
                    // so scalar element types are wrapped as `_value:<scalar>`,
                    // matching how `schema_for_source` records the source's
                    // schema in the lockfile. Otherwise scalar / relation-of-
                    // scalar source migrations could never match the lockfile
                    // (`_value:int` vs `int`) and always failed validation.
                    let old_schema = relation_inner_schema(&old_resolved);
                    let new_schema = relation_inner_schema(&new_resolved);
                    migrate_schemas
                        .entry(relation.clone())
                        .or_default()
                        .push((old_schema, new_schema));
                }
                DeclKind::Derived { name, ty: Some(scheme), .. } => {
                    // Compute schema for derived relations with type annotations
                    // so groupBy can use them
                    let schema =
                        schema_for_source(&scheme.ty, &aliases, &associated_types, &single_variant_params);
                    source_schemas.insert(name.clone(), schema);
                }
                DeclKind::SubsetConstraint { sub, sup } => {
                    subset_constraints.push((sub.clone(), sup.clone()));
                }
                _ => {}
            }
        }

        Self {
            aliases,
            constructors,
            source_schemas,
            migrate_schemas,
            associated_types,
            subset_constraints,
            source_refinements,
            refined_types,
        }
    }
}

/// Detect cyclic type-alias definitions: direct (`type A = {x: A}`),
/// mutual (`type A = B; type B = A`), and through any type structure
/// (records, relations, functions, refined bases, ...). Returns one error
/// diagnostic per cyclic alias.
///
/// Must run before `TypeEnv::from_module`'s resolution: a cyclic alias can
/// never be fully resolved, and downstream consumers (schema generation,
/// codegen) have no representation for infinite types.
pub fn check_alias_cycles(module: &Module) -> Vec<knot::diagnostic::Diagnostic> {
    let mut alias_decls: Vec<(String, &Type, Span)> = Vec::new();
    for decl in &module.decls {
        if let DeclKind::TypeAlias { name, params, ty } = &decl.node
            && params.is_empty() {
                alias_decls.push((name.clone(), ty, decl.span));
            }
    }
    let alias_names: HashSet<String> =
        alias_decls.iter().map(|(n, _, _)| n.clone()).collect();
    let mut deps: HashMap<String, HashSet<String>> = HashMap::new();
    for (name, ty, _) in &alias_decls {
        let mut refs = HashSet::new();
        collect_named_alias_refs(ty, &alias_names, &mut refs);
        deps.entry(name.clone()).or_default().extend(refs);
    }
    let mut diags = Vec::new();
    for (name, _, span) in &alias_decls {
        // The alias is cyclic when it can reach itself through alias refs.
        let mut stack: Vec<String> = deps[name].iter().cloned().collect();
        let mut visited: HashSet<String> = HashSet::new();
        let mut found = false;
        while let Some(n) = stack.pop() {
            if &n == name {
                found = true;
                break;
            }
            if visited.insert(n.clone())
                && let Some(ds) = deps.get(&n) {
                    stack.extend(ds.iter().cloned());
                }
        }
        if found {
            diags.push(
                knot::diagnostic::Diagnostic::error(format!(
                    "recursive type alias '{}' — a type alias cannot refer \
                     to itself, directly or through other aliases",
                    name
                ))
                .label(*span, "cycle detected in this type alias"),
            );
        }
    }
    diags
}

/// Collect the alias names referenced anywhere inside a type AST.
fn collect_named_alias_refs(
    ty: &Type,
    alias_names: &HashSet<String>,
    out: &mut HashSet<String>,
) {
    match &ty.node {
        TypeKind::Named(name) => {
            if alias_names.contains(name) {
                out.insert(name.clone());
            }
        }
        TypeKind::Var(_) | TypeKind::Hole => {}
        TypeKind::App { func, arg } => {
            collect_named_alias_refs(func, alias_names, out);
            collect_named_alias_refs(arg, alias_names, out);
        }
        TypeKind::Record { fields, .. } => {
            for f in fields {
                collect_named_alias_refs(&f.value, alias_names, out);
            }
        }
        TypeKind::Relation(inner) => {
            collect_named_alias_refs(inner, alias_names, out);
        }
        TypeKind::Function { param, result } => {
            collect_named_alias_refs(param, alias_names, out);
            collect_named_alias_refs(result, alias_names, out);
        }
        TypeKind::Variant { constructors, .. } => {
            for c in constructors {
                for f in &c.fields {
                    collect_named_alias_refs(&f.value, alias_names, out);
                }
            }
        }
        TypeKind::Effectful { ty, .. } | TypeKind::IO { ty, .. } => {
            collect_named_alias_refs(ty, alias_names, out);
        }
        TypeKind::UnitAnnotated { base, .. } | TypeKind::Refined { base, .. } => {
            collect_named_alias_refs(base, alias_names, out);
        }
        TypeKind::Forall { ty, .. } => {
            collect_named_alias_refs(ty, alias_names, out);
        }
    }
}

/// Walk a source's type (e.g., `[{name: NonEmptyText, age: Nat}]`) and collect
/// refinement info: (field_name_or_none, refined_type_name, predicate_expr).
///
/// Nested refinements (fields of nested relations/records, ADT constructor
/// fields, stacked inline-over-alias predicates) are collected by
/// synthesizing wrapper predicates over the *top-level* field value — the
/// runtime's `knot_refinement_validate_relation` only extracts one field
/// level, so deeper paths are folded into the predicate itself (`all`
/// over nested relations, field access for nested records, `case` for
/// constructor payloads). Validation runs on the in-memory rows before the
/// parent write, so the wrappers see the full nested structure.
fn collect_source_refinements(
    ty: &Type,
    refined_types: &HashMap<String, Expr>,
    alias_ast_types: &HashMap<String, Type>,
    data_ctor_decls: &HashMap<String, Vec<ConstructorDef>>,
) -> Vec<(Option<String>, String, Expr)> {
    collect_source_refinements_inner(
        ty,
        refined_types,
        alias_ast_types,
        data_ctor_decls,
        &mut HashSet::new(),
    )
}

fn collect_source_refinements_inner(
    ty: &Type,
    refined_types: &HashMap<String, Expr>,
    alias_ast_types: &HashMap<String, Type>,
    data_ctor_decls: &HashMap<String, Vec<ConstructorDef>>,
    seen_aliases: &mut HashSet<String>,
) -> Vec<(Option<String>, String, Expr)> {
    let mut result = Vec::new();
    // Unwrap [T] to get to the element type
    let elem_ty = match &ty.node {
        TypeKind::Relation(inner) => inner.as_ref(),
        _ => ty,
    };
    match &elem_ty.node {
        // Element type is a refined type alias: *scores : [Nat]
        TypeKind::Named(name) if refined_types.contains_key(name) => {
            result.push((None, name.clone(), refined_types[name].clone()));
            // The alias base may itself carry field-level refinements
            // (`type P = {age: Int where ...} where ...`) — recurse into it
            // so those aren't dropped (mirrors the inline Refined branch).
            if seen_aliases.insert(name.clone()) {
                if let Some(alias_ty) = alias_ast_types.get(name) {
                    let base: &Type = match &alias_ty.node {
                        TypeKind::Refined { base, .. } => base.as_ref(),
                        _ => alias_ty,
                    };
                    let inner_ty = if matches!(&base.node, TypeKind::Relation(_)) {
                        base.clone()
                    } else {
                        Spanned::new(TypeKind::Relation(Box::new(base.clone())), ty.span)
                    };
                    result.extend(collect_source_refinements_inner(
                        &inner_ty,
                        refined_types,
                        alias_ast_types,
                        data_ctor_decls,
                        seen_aliases,
                    ));
                }
                seen_aliases.remove(name);
            }
        }
        // Element type is a type alias: *people : [Person]
        // Resolve through the alias to find refined fields in the underlying record.
        TypeKind::Named(name) if alias_ast_types.contains_key(name)
            // Guard against cyclic aliases (`type A = B; type B = A`):
            // without a seen-set this recursion never terminates. The cycle
            // itself is reported as a diagnostic by type inference.
            && seen_aliases.insert(name.clone()) => {
                let alias_ty = &alias_ast_types[name];
                // If the alias already resolves to a Relation type (e.g. `type People = [{name: Nat}]`),
                // recurse directly to avoid double-wrapping Relation(Relation(...)).
                if matches!(&alias_ty.node, TypeKind::Relation(_)) {
                    result.extend(collect_source_refinements_inner(alias_ty, refined_types, alias_ast_types, data_ctor_decls, seen_aliases));
                } else {
                    let inner_ty = Spanned::new(TypeKind::Relation(Box::new(alias_ty.clone())), ty.span);
                    result.extend(collect_source_refinements_inner(&inner_ty, refined_types, alias_ast_types, data_ctor_decls, seen_aliases));
                }
                seen_aliases.remove(name);
            }
        // Element type is a multi-variant data type: *shapes : [Shape] with
        // constructor field refinements like `Circle {radius: Float where ...}`.
        // Each refinement becomes a whole-element predicate that matches the
        // constructor and checks the payload field (other constructors pass).
        TypeKind::Named(name) if data_ctor_decls.contains_key(name) => {
            for (label, pred) in adt_value_predicates(
                name,
                refined_types,
                alias_ast_types,
                data_ctor_decls,
                seen_aliases,
                elem_ty.span,
            ) {
                result.push((None, label, pred));
            }
        }
        // Element type is a record: check each field
        TypeKind::Record { fields, .. } => {
            for field in fields {
                for (label, pred) in field_value_predicates(
                    &field.name,
                    &field.value,
                    refined_types,
                    alias_ast_types,
                    data_ctor_decls,
                    seen_aliases,
                ) {
                    result.push((Some(field.name.clone()), label, pred));
                }
            }
        }
        // Element type is a refined record: {..} where \p -> ...
        TypeKind::Refined { base, predicate } => {
            // Collect the cross-field predicate
            result.push((None, "record".into(), (**predicate).clone()));
            // Recurse into the base to collect field-level refinements. If the
            // base is already a Relation (a refined *nested* relation element,
            // e.g. `[[Item] where \xs -> ...]`), recurse directly to avoid
            // double-wrapping Relation(Relation(...)) — which the single-unwrap
            // recursion would leave as a bare Relation, matching no arm and
            // silently dropping the inner element/field refinements. Mirrors the
            // guarded `Named`-alias arms above.
            let inner_ty = if matches!(&base.node, TypeKind::Relation(_)) {
                (**base).clone()
            } else {
                Spanned::new(TypeKind::Relation(base.clone()), ty.span)
            };
            result.extend(collect_source_refinements_inner(&inner_ty, refined_types, alias_ast_types, data_ctor_decls, seen_aliases));
        }
        _ => {}
    }
    result
}

/// Predicates enforcing the declared refinements of a record FIELD, each
/// taking the field's value directly: `(type_label, \fieldValue -> Bool)`.
///
/// Covers: inline refinements (`age: Int where ...` — label is the field
/// name, matching the historical message), refined aliases reached through
/// plain alias chains (`age: Nat`), stacked inline-over-refined-alias
/// (`age: Nat where ...` — both predicates), nested record aliases
/// (`addr: Addr` with refined fields inside `Addr`), and nested relations
/// (`items: [{qty: Pos}]` — wrapped in `all`).
fn field_value_predicates(
    field_name: &str,
    field_ty: &Type,
    refined_types: &HashMap<String, Expr>,
    alias_ast_types: &HashMap<String, Type>,
    data_ctor_decls: &HashMap<String, Vec<ConstructorDef>>,
    seen_aliases: &mut HashSet<String>,
) -> Vec<(String, Expr)> {
    match &field_ty.node {
        TypeKind::Refined { base, predicate } => {
            let mut out = vec![(field_name.to_string(), (**predicate).clone())];
            // Stacked refinement: the base may itself be refined (e.g.
            // `age: Nat where \x -> x < 150` must also enforce Nat's
            // predicate), or carry nested refinements.
            out.extend(value_predicates(
                base,
                refined_types,
                alias_ast_types,
                data_ctor_decls,
                seen_aliases,
            ));
            out
        }
        _ => value_predicates(
            field_ty,
            refined_types,
            alias_ast_types,
            data_ctor_decls,
            seen_aliases,
        ),
    }
}

/// Predicates enforcing the declared refinements of a VALUE of type `ty`,
/// each taking the value directly: `(type_label, \value -> Bool)`.
fn value_predicates(
    ty: &Type,
    refined_types: &HashMap<String, Expr>,
    alias_ast_types: &HashMap<String, Type>,
    data_ctor_decls: &HashMap<String, Vec<ConstructorDef>>,
    seen_aliases: &mut HashSet<String>,
) -> Vec<(String, Expr)> {
    let mut out = Vec::new();
    match &ty.node {
        // Anonymous refinement of the whole value.
        TypeKind::Refined { base, predicate } => {
            out.push(("record".to_string(), (**predicate).clone()));
            out.extend(value_predicates(
                base,
                refined_types,
                alias_ast_types,
                data_ctor_decls,
                seen_aliases,
            ));
        }
        TypeKind::Named(name)
            if seen_aliases.insert(name.clone()) => {
                if let Some(pred) = refined_types.get(name) {
                    out.push((name.clone(), pred.clone()));
                    // The refined alias's base may carry deeper refinements
                    // (`type Addr = {zip: Zip} where ...`).
                    if let Some(alias_ty) = alias_ast_types.get(name) {
                        let base: &Type = match &alias_ty.node {
                            TypeKind::Refined { base, .. } => base.as_ref(),
                            _ => alias_ty,
                        };
                        out.extend(value_predicates(
                            base,
                            refined_types,
                            alias_ast_types,
                            data_ctor_decls,
                            seen_aliases,
                        ));
                    }
                } else if let Some(alias_ty) = alias_ast_types.get(name) {
                    out.extend(value_predicates(
                        alias_ty,
                        refined_types,
                        alias_ast_types,
                        data_ctor_decls,
                        seen_aliases,
                    ));
                } else if data_ctor_decls.contains_key(name) {
                    out.extend(adt_value_predicates(
                        name,
                        refined_types,
                        alias_ast_types,
                        data_ctor_decls,
                        seen_aliases,
                        ty.span,
                    ));
                }
                seen_aliases.remove(name);
            }
        // Nested record: wrap each field predicate with a field access.
        TypeKind::Record { fields, .. } => {
            for field in fields {
                for (label, pred) in field_value_predicates(
                    &field.name,
                    &field.value,
                    refined_types,
                    alias_ast_types,
                    data_ctor_decls,
                    seen_aliases,
                ) {
                    let span = field.value.span;
                    let param = synth_fresh_name();
                    let body = synth_app(
                        pred,
                        synth_field(synth_var(&param, span), &field.name),
                    );
                    out.push((label, synth_lambda(&param, body)));
                }
            }
        }
        // Nested relation: every element must satisfy the element predicates.
        TypeKind::Relation(inner) => {
            for (label, pred) in value_predicates(
                inner,
                refined_types,
                alias_ast_types,
                data_ctor_decls,
                seen_aliases,
            ) {
                let span = inner.span;
                let param = synth_fresh_name();
                // \v -> all pred v
                let body = synth_app(
                    synth_app(synth_var("all", span), pred),
                    synth_var(&param, span),
                );
                out.push((label, synth_lambda(&param, body)));
            }
        }
        TypeKind::UnitAnnotated { base, .. } => {
            out.extend(value_predicates(
                base,
                refined_types,
                alias_ast_types,
                data_ctor_decls,
                seen_aliases,
            ));
        }
        // Type application such as `Maybe Nat` or `Result Text Pos`. The
        // refinement lives on a type *argument*, but the runtime value is
        // wrapped in one of the type's constructors (`Just {value: x}`,
        // `Ok {value: x}`, `Err {error: e}`), so applying the inner predicate
        // to the whole value would compare a constructor against a primitive
        // and panic. Recurse into each argument and wrap the resulting
        // predicate in a `case` that unwraps the constructor carrying that
        // argument and passes for the other variants (`Nothing`, the other
        // arm of `Result`) — mirroring the `all` wrap used for relations.
        TypeKind::App { .. } => {
            // Flatten the left-nested application spine to (head, [args]).
            let mut head: &Type = ty;
            let mut args: Vec<&Type> = Vec::new();
            while let TypeKind::App { func, arg } = &head.node {
                args.push(arg.as_ref());
                head = func.as_ref();
            }
            args.reverse();
            if let TypeKind::Named(head_name) = &head.node {
                for (idx, arg_ty) in args.iter().enumerate() {
                    // (constructor, payload field) carrying this positional
                    // argument, for the built-in generics whose shape is
                    // fixed. Unknown heads collect nothing (safe fallback).
                    let ctor_field = match (head_name.as_str(), idx) {
                        ("Maybe", 0) => Some(("Just", "value")),
                        ("Result", 0) => Some(("Err", "error")),
                        ("Result", 1) => Some(("Ok", "value")),
                        _ => None,
                    };
                    let Some((ctor, field)) = ctor_field else {
                        continue;
                    };
                    for (label, pred) in value_predicates(
                        arg_ty,
                        refined_types,
                        alias_ast_types,
                        data_ctor_decls,
                        seen_aliases,
                    ) {
                        out.push((
                            label,
                            synth_ctor_field_case(ctor, field, pred, arg_ty.span),
                        ));
                    }
                }
            }
        }
        _ => {}
    }
    out
}

/// Whole-element predicates for a multi-variant data type with refined
/// constructor fields: each predicate matches one constructor, checks one
/// payload field, and passes every other constructor.
fn adt_value_predicates(
    data_name: &str,
    refined_types: &HashMap<String, Expr>,
    alias_ast_types: &HashMap<String, Type>,
    data_ctor_decls: &HashMap<String, Vec<ConstructorDef>>,
    seen_aliases: &mut HashSet<String>,
    span: Span,
) -> Vec<(String, Expr)> {
    let mut out = Vec::new();
    let Some(ctors) = data_ctor_decls.get(data_name) else {
        return out;
    };
    for ctor in ctors {
        for field in &ctor.fields {
            for (label, pred) in field_value_predicates(
                &field.name,
                &field.value,
                refined_types,
                alias_ast_types,
                data_ctor_decls,
                seen_aliases,
            ) {
                out.push((
                    format!("{}.{}.{}", data_name, ctor.name, label),
                    synth_ctor_field_case(&ctor.name, &field.name, pred, span),
                ));
            }
        }
    }
    out
}

// ── Synthesized predicate builders for nested refinements ─────────

/// Globally unique parameter names for synthesized lambdas, so nested
/// wrappers never shadow each other.
fn synth_fresh_name() -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static SYNTH_COUNTER: AtomicUsize = AtomicUsize::new(0);
    format!("__refine_v{}", SYNTH_COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn synth_var(name: &str, span: Span) -> Expr {
    Spanned::new(ExprKind::Var(name.into()), span)
}

fn synth_app(f: Expr, a: Expr) -> Expr {
    let span = f.span;
    Spanned::new(
        ExprKind::App {
            func: Box::new(f),
            arg: Box::new(a),
        },
        span,
    )
}

fn synth_lambda(param: &str, body: Expr) -> Expr {
    let span = body.span;
    Spanned::new(
        ExprKind::Lambda {
            params: vec![Spanned::new(PatKind::Var(param.into()), span)],
            body: Box::new(body),
        },
        span,
    )
}

fn synth_field(e: Expr, field: &str) -> Expr {
    let span = e.span;
    Spanned::new(
        ExprKind::FieldAccess {
            expr: Box::new(e),
            field: field.into(),
        },
        span,
    )
}

/// `\v -> case v of Ctor {field: f} -> pred f | _ -> true`
fn synth_ctor_field_case(ctor: &str, field: &str, pred: Expr, span: Span) -> Expr {
    let scrut_param = synth_fresh_name();
    let payload_param = synth_fresh_name();
    let bound = Spanned::new(PatKind::Var(payload_param.clone()), span);
    let rec_pat = Spanned::new(
        PatKind::Record(vec![FieldPat {
            name: field.into(),
            name_span: span,
            pattern: Some(bound),
        }]),
        span,
    );
    let ctor_pat = Spanned::new(
        PatKind::Constructor {
            name: ctor.into(),
            payload: Box::new(rec_pat),
        },
        span,
    );
    let match_arm = CaseArm {
        pat: ctor_pat,
        body: synth_app(pred, synth_var(&payload_param, span)),
    };
    let pass_arm = CaseArm {
        pat: Spanned::new(PatKind::Wildcard, span),
        body: Spanned::new(ExprKind::Lit(Literal::Bool(true)), span),
    };
    let case = Spanned::new(
        ExprKind::Case {
            scrutinee: Box::new(synth_var(&scrut_param, span)),
            arms: vec![match_arm, pass_arm],
        },
        span,
    );
    synth_lambda(&scrut_param, case)
}

#[allow(clippy::type_complexity)]
fn resolve_type(
    ty: &Type,
    aliases: &HashMap<String, ResolvedType>,
    assoc_types: &HashMap<String, Vec<AssocTypeDef>>,
    single_variant_params: &HashMap<String, (Vec<String>, Vec<(String, Type)>)>,
) -> ResolvedType {
    match &ty.node {
        TypeKind::Named(name) => match name.as_str() {
            "Int" => ResolvedType::Int,
            "Float" => ResolvedType::Float,
            "Text" => ResolvedType::Text,
            "Bool" => ResolvedType::Bool,
            "Bytes" => ResolvedType::Bytes,
            "Uuid" => ResolvedType::Uuid,
            _ => aliases
                .get(name)
                .cloned()
                .unwrap_or(ResolvedType::Named(name.clone())),
        },
        TypeKind::Record { fields, .. } => {
            let resolved: Vec<(String, ResolvedType)> = fields
                .iter()
                .map(|f| {
                    (f.name.clone(), resolve_type(&f.value, aliases, assoc_types, single_variant_params))
                })
                .collect();
            ResolvedType::Record(resolved)
        }
        TypeKind::Relation(inner) => ResolvedType::Relation(Box::new(
            resolve_type(inner, aliases, assoc_types, single_variant_params),
        )),
        TypeKind::Function { param, result } => ResolvedType::Function(
            Box::new(resolve_type(param, aliases, assoc_types, single_variant_params)),
            Box::new(resolve_type(result, aliases, assoc_types, single_variant_params)),
        ),
        TypeKind::Var(_) => ResolvedType::Named("unknown".into()),
        TypeKind::App { func, arg } => {
            // Collect the full application spine. A multi-argument application
            // `Convert X Y` parses as `App(App(Convert, X), Y)`, so peel the
            // nested funcs to recover the head constructor and its arguments
            // left-to-right.
            let mut spine_head = func.as_ref();
            let mut spine_args: Vec<&Type> = vec![arg.as_ref()];
            while let TypeKind::App { func: inner, arg: inner_arg } = &spine_head.node {
                spine_args.push(inner_arg.as_ref());
                spine_head = inner;
            }
            spine_args.reverse();

            // Associated type application: match the whole spine against a
            // definition of matching arity, binding ALL argument patterns into
            // a SINGLE substitution. Matching only the first argument (the old
            // behaviour) ignored `def.args[1..]`, leaving their pattern vars
            // unsubstituted in the result and picking the wrong instance for
            // multi-parameter associated types.
            if let TypeKind::Named(name) = &spine_head.node
                && let Some(defs) = assoc_types.get(name) {
                    for def in defs {
                        if !def.args.is_empty() && def.args.len() == spine_args.len() {
                            let mut subst = HashMap::new();
                            if def
                                .args
                                .iter()
                                .zip(spine_args.iter())
                                .all(|(pat, concrete)| {
                                    match_type_pattern(pat, concrete, &mut subst)
                                })
                            {
                                let resolved_ty = apply_type_subst(&def.ty, &subst);
                                return resolve_type(&resolved_ty, aliases, assoc_types, single_variant_params);
                            }
                        }
                    }
                }
            // Parameterized ADT (e.g. `Maybe Text`, `Result E A`, user data
            // types with type args): resolve to the ADT shape so the schema
            // maps the field to the "json" column type, which round-trips
            // constructors faithfully via the `__knot_ctor` marker
            // (`value_to_json`/`json_to_value`). Previously this fell
            // through to Named("unknown") → "text", which persisted only
            // the constructor tag and silently corrupted data.
            if let TypeKind::Named(name) = &func.node {
                // Built-in ADTs aren't in `aliases` — build their shapes
                // directly, substituting the actual type argument.
                if name == "Maybe" {
                    let arg_ty = resolve_type(arg, aliases, assoc_types, single_variant_params);
                    return ResolvedType::Adt(vec![
                        ("Nothing".into(), vec![]),
                        ("Just".into(), vec![("value".into(), arg_ty)]),
                    ]);
                }
            }
            // `Result e a` arrives as App(App(Result, e), a).
            if let TypeKind::App { func: inner_func, arg: err_arg } = &func.node
                && matches!(&inner_func.node, TypeKind::Named(n) if n == "Result") {
                    let err_ty = resolve_type(err_arg, aliases, assoc_types, single_variant_params);
                    let ok_ty = resolve_type(arg, aliases, assoc_types, single_variant_params);
                    return ResolvedType::Adt(vec![
                        ("Err".into(), vec![("error".into(), err_ty)]),
                        ("Ok".into(), vec![("value".into(), ok_ty)]),
                    ]);
                }
            // User-declared parameterized data types: the head's registered
            // shape is enough to pick the column type (payload-bearing ADTs
            // become "json" columns regardless of the type arguments).
            // Single-variant data types register as a `Record` (see
            // `collect_types`), so accept that too — otherwise an applied
            // form like `Box Int` falls through to Named("unknown") → "text"
            // and silently corrupts the structure, while the bare `Box`
            // (resolved via `aliases` to a Record → "json") round-trips fine.
            //
            // For *parameterized* single-variant data types
            // (`data Box a = Box {value: a}` applied as `Box Int`), the
            // stored alias was resolved with type parameters replaced by
            // `Named("unknown")`. Re-substitute the actual type arguments
            // into the original field types so a `Box Int` source gets the
            // column type of `Int` (e.g. "value:int") rather than the
            // meaningless "value:text".
            if let TypeKind::Named(name) = &spine_head.node
                && let Some((params, field_types)) = single_variant_params.get(name)
                && params.len() == spine_args.len()
            {
                let subst: HashMap<String, Type> =
                    params.iter().zip(spine_args.iter())
                    .map(|(p, arg)| (p.clone(), (*arg).clone()))
                    .collect();
                let substituted: Vec<(String, ResolvedType)> = field_types
                    .iter()
                    .map(|(fname, fty)| {
                        let substituted_ty = apply_type_subst(fty, &subst);
                        (fname.clone(), resolve_type(&substituted_ty, aliases, assoc_types, single_variant_params))
                    })
                    .collect();
                return ResolvedType::Record(substituted);
            }
            if let TypeKind::Named(name) = &spine_head.node
                && let Some(
                    resolved @ (ResolvedType::Adt(_) | ResolvedType::Record(_)),
                ) = aliases.get(name)
                {
                    return resolved.clone();
                }
            ResolvedType::Named("unknown".into())
        }
        TypeKind::Hole => ResolvedType::Named("unknown".into()),
        TypeKind::Variant { constructors, .. } => {
            let ctors: Vec<(String, Vec<(String, ResolvedType)>)> =
                constructors
                    .iter()
                    .map(|c| {
                        let fields: Vec<(String, ResolvedType)> = c
                            .fields
                            .iter()
                            .map(|f| {
                                (
                                    f.name.clone(),
                                    resolve_type(
                                        &f.value, aliases, assoc_types, single_variant_params,
                                    ),
                                )
                            })
                            .collect();
                        (c.name.clone(), fields)
                    })
                    .collect();
            ResolvedType::Adt(ctors)
        }
        TypeKind::Effectful { ty, .. } => {
            resolve_type(ty, aliases, assoc_types, single_variant_params)
        }
        TypeKind::IO { ty, .. } => {
            // IO values aren't persisted — resolve inner type for diagnostics
            resolve_type(ty, aliases, assoc_types, single_variant_params)
        }
        TypeKind::UnitAnnotated { base, .. } => {
            // Units are phantom — erase for schema resolution
            resolve_type(base, aliases, assoc_types, single_variant_params)
        }
        TypeKind::Refined { base, .. } => {
            // Refinements are phantom for schema — base type determines storage
            resolve_type(base, aliases, assoc_types, single_variant_params)
        }
        TypeKind::Forall { ty, .. } => {
            // Quantifiers are phantom for schema resolution.
            resolve_type(ty, aliases, assoc_types, single_variant_params)
        }
    }
}

/// Re-resolve a `ResolvedType`, replacing `Named(x)` with the alias if now known.
/// Recurses into the replacement so nested `Named` refs are also resolved.
/// Uses `seen` to break cycles from mutually recursive types.
fn re_resolve_type(ty: &ResolvedType, aliases: &HashMap<String, ResolvedType>) -> ResolvedType {
    re_resolve_inner(ty, aliases, &mut HashSet::new())
}

fn re_resolve_inner(ty: &ResolvedType, aliases: &HashMap<String, ResolvedType>, seen: &mut HashSet<String>) -> ResolvedType {
    match ty {
        ResolvedType::Named(name) => {
            if seen.contains(name) {
                return ty.clone(); // break cycle
            }
            match aliases.get(name) {
                Some(resolved) => {
                    seen.insert(name.clone());
                    let result = re_resolve_inner(resolved, aliases, seen);
                    seen.remove(name);
                    result
                }
                None => ty.clone(),
            }
        }
        ResolvedType::Record(fields) => ResolvedType::Record(
            fields
                .iter()
                .map(|(n, t)| (n.clone(), re_resolve_inner(t, aliases, seen)))
                .collect(),
        ),
        ResolvedType::Relation(inner) => {
            ResolvedType::Relation(Box::new(re_resolve_inner(inner, aliases, seen)))
        }
        ResolvedType::Function(a, b) => ResolvedType::Function(
            Box::new(re_resolve_inner(a, aliases, seen)),
            Box::new(re_resolve_inner(b, aliases, seen)),
        ),
        ResolvedType::Adt(ctors) => ResolvedType::Adt(
            ctors
                .iter()
                .map(|(name, fields)| {
                    (
                        name.clone(),
                        fields
                            .iter()
                            .map(|(n, t)| (n.clone(), re_resolve_inner(t, aliases, seen)))
                            .collect(),
                    )
                })
                .collect(),
        ),
        _ => ty.clone(),
    }
}

#[allow(clippy::type_complexity)]
fn schema_for_source(
    ty: &Type,
    aliases: &HashMap<String, ResolvedType>,
    assoc_types: &HashMap<String, Vec<AssocTypeDef>>,
    single_variant_params: &HashMap<String, (Vec<String>, Vec<(String, Type)>)>,
) -> String {
    // Unwrap Effectful/Refined wrappers to find the underlying type
    let unwrapped = match &ty.node {
        TypeKind::Effectful { ty: inner, .. } | TypeKind::Refined { base: inner, .. } => inner.as_ref(),
        _ => ty,
    };
    match &unwrapped.node {
        TypeKind::Relation(inner) => {
            let resolved = resolve_type(inner, aliases, assoc_types, single_variant_params);
            relation_inner_schema(&resolved)
        }
        _ => {
            // The type might be a named alias that resolves to a Relation
            // (e.g. `type People = [{name: Text}]` and `*people : People`).
            // Check the resolved type before falling back to scalar schema.
            let resolved = resolve_type(unwrapped, aliases, assoc_types, single_variant_params);
            if let ResolvedType::Relation(inner) = &resolved {
                return relation_inner_schema(inner);
            }
            // Non-relation source type (e.g. `*counter : Int`):
            // wrap as a single-column `_value` schema.
            format!("_value:{}", col_type_str(&resolved))
        }
    }
}

/// Schema for the inner type of a relation. Records/ADTs delegate to
/// `schema_descriptor`; primitive inner types (e.g. `*tags : [Text]`) get
/// wrapped as a single-column `_value:<scalar>` schema, matching how scalar
/// sources are stored. Without this wrapping the runtime sees a bare type
/// name like `"text"` and panics in `parse_record_schema`.
fn relation_inner_schema(inner: &ResolvedType) -> String {
    match inner {
        ResolvedType::Record(_) | ResolvedType::Adt(_) => schema_descriptor(inner),
        // A relation-of-relations element (`*tags : [[Text]]`, `*grid : [[R]]`)
        // is itself a relation and can never live in a scalar column. Store it
        // in a single `_value:json` column, the same round-trip used for
        // relation-typed *record* fields (`format_schema_field`). Without this,
        // `col_type_str` falls through to `"text"`, the schema becomes
        // `_value:text`, the program compiles clean, and every write panics at
        // runtime ("cannot convert Relation to SQL").
        ResolvedType::Relation(_) => "_value:json".to_string(),
        _ => format!("_value:{}", col_type_str(inner)),
    }
}

/// Column type string for a resolved type used as a record field.
fn col_type_str(ty: &ResolvedType) -> &'static str {
    match ty {
        ResolvedType::Int => "int",
        ResolvedType::Float => "float",
        ResolvedType::Text => "text",
        ResolvedType::Bool => "bool",
        ResolvedType::Bytes => "bytes",
        ResolvedType::Uuid => "text",
        ResolvedType::Adt(ctors) => {
            // Enum-like ADTs (all nullary) get the "tag" type
            if ctors.iter().all(|(_, fields)| fields.is_empty()) {
                "tag"
            } else {
                "json" // payload-bearing ADTs round-trip through JSON
            }
        }
        // Nested records round-trip through JSON to preserve structure
        ResolvedType::Record(_) => "json",
        _ => "text",
    }
}

/// Format a single field for a schema descriptor.
///
/// Contract with the runtime's `parse_record_schema`:
/// - Nested relations of *record* element type are inlined as
///   `field:[child_schema]` and stored in child tables.
/// - Nested relations of any other element type (ADTs, scalars) are stored
///   as a `json` column holding the whole relation. Child tables only
///   support record-shaped rows (`_parent_id` + scalar columns), so the
///   `[...]` form must not be emitted for non-record elements — the runtime
///   used to panic at table init on descriptors like `tags:[text]` or
///   `shapes:[#Circle:...|Dot]`. The Json column round-trip
///   (`value_to_json`/`json_to_value` with the `__knot_ctor` marker)
///   reconstructs relations of constructors and scalars faithfully, and
///   `m <- t.field` binds iterate the in-memory relation read back from
///   the column.
fn format_schema_field(name: &str, ty: &ResolvedType) -> String {
    if let ResolvedType::Relation(inner) = ty {
        match inner.as_ref() {
            ResolvedType::Record(_) => {
                format!("{}:[{}]", name, schema_descriptor(inner))
            }
            _ => format!("{}:json", name),
        }
    } else {
        format!("{}:{}", name, col_type_str(ty))
    }
}

fn schema_descriptor(ty: &ResolvedType) -> String {
    match ty {
        ResolvedType::Record(fields) => fields
            .iter()
            .map(|(name, ty)| format_schema_field(name, ty))
            .collect::<Vec<_>>()
            .join(","),
        ResolvedType::Adt(ctors) => {
            // Direct ADT relation: generate #Ctor1:f1=t1;f2=t2|Ctor2|...
            let parts: Vec<String> = ctors
                .iter()
                .map(|(ctor_name, fields)| {
                    if fields.is_empty() {
                        ctor_name.clone()
                    } else {
                        let field_specs: Vec<String> = fields
                            .iter()
                            .map(|(fname, fty)| {
                                if let ResolvedType::Relation(inner) = fty {
                                    format!("{}=[{}]", fname, schema_descriptor(inner))
                                } else {
                                    format!("{}={}", fname, col_type_str(fty))
                                }
                            })
                            .collect();
                        format!("{}:{}", ctor_name, field_specs.join(";"))
                    }
                })
                .collect();
            format!("#{}", parts.join("|"))
        }
        other => col_type_str(other).to_string(),
    }
}

// ── Associated type resolution helpers ────────────────────────────

/// Match a concrete type against a pattern type, building a substitution.
/// Pattern variables (TypeKind::Var) match anything and bind the concrete type.
fn match_type_pattern(
    pattern: &Type,
    concrete: &Type,
    subst: &mut HashMap<String, Type>,
) -> bool {
    match (&pattern.node, &concrete.node) {
        // Type variables match anything — but a repeated (non-linear) pattern
        // variable must bind consistently. `Pair a a` must NOT match
        // `Pair Int Text`; without this check the second `a` silently
        // overwrites the first, so `match_type_pattern` would accept the
        // mismatched type and select the wrong associated-type instance /
        // schema column type. The prior binding is itself a concrete type (no
        // pattern vars), so matching it against the new concrete reduces to a
        // structural-equality check.
        (TypeKind::Var(name), _) => {
            if let Some(existing) = subst.get(name) {
                // The prior binding is the concrete type captured at an earlier
                // occurrence of this pattern variable. The second occurrence must
                // be structurally identical. When the prior binding is itself a
                // type variable (from a polymorphic concrete context), we must
                // NOT treat its name as a fresh pattern variable — doing so would
                // accept distinct variables (e.g., `Pair a a` matching `Pair x y`).
                match &existing.node {
                    TypeKind::Var(prev_name) => {
                        matches!(&concrete.node, TypeKind::Var(n) if n == prev_name)
                    }
                    _ => {
                        let mut tmp = HashMap::new();
                        match_type_pattern(existing, concrete, &mut tmp)
                    }
                }
            } else {
                subst.insert(name.clone(), concrete.clone());
                true
            }
        }
        // Named types must match exactly
        (TypeKind::Named(a), TypeKind::Named(b)) => a == b,
        // Relation types recurse on the inner type
        (TypeKind::Relation(p_inner), TypeKind::Relation(c_inner)) => {
            match_type_pattern(p_inner, c_inner, subst)
        }
        // Record types match field-by-field, by name (not positionally)
        (
            TypeKind::Record { fields: pf, .. },
            TypeKind::Record { fields: cf, .. },
        ) => {
            if pf.len() != cf.len() {
                return false;
            }
            pf.iter().all(|p| {
                cf.iter().any(|c| {
                    p.name == c.name
                        && match_type_pattern(&p.value, &c.value, subst)
                })
            })
        }
        // Type applications recurse on both parts
        (
            TypeKind::App { func: pf, arg: pa },
            TypeKind::App { func: cf, arg: ca },
        ) => {
            match_type_pattern(pf, cf, subst)
                && match_type_pattern(pa, ca, subst)
        }
        // Function types recurse on param and result
        (
            TypeKind::Function {
                param: pp,
                result: pr,
            },
            TypeKind::Function {
                param: cp,
                result: cr,
            },
        ) => {
            match_type_pattern(pp, cp, subst)
                && match_type_pattern(pr, cr, subst)
        }
        _ => false,
    }
}

/// Apply a type variable substitution to a type AST node.
fn apply_type_subst(ty: &Type, subst: &HashMap<String, Type>) -> Type {
    let new_node = match &ty.node {
        TypeKind::Var(name) => {
            if let Some(replacement) = subst.get(name) {
                return replacement.clone();
            }
            ty.node.clone()
        }
        TypeKind::Named(_) => ty.node.clone(),
        TypeKind::Relation(inner) => {
            TypeKind::Relation(Box::new(apply_type_subst(inner, subst)))
        }
        TypeKind::Record { fields, rest } => TypeKind::Record {
            fields: fields
                .iter()
                .map(|f| Field {
                    name: f.name.clone(),
                    value: apply_type_subst(&f.value, subst),
                })
                .collect(),
            rest: rest.as_ref().and_then(|r| {
                if let Some(replacement) = subst.get(r) {
                    if let TypeKind::Var(new_name) = &replacement.node {
                        Some(new_name.clone())
                    } else {
                        None // row resolved to concrete type — close the row
                    }
                } else {
                    Some(r.clone())
                }
            }),
        },
        TypeKind::App { func, arg } => TypeKind::App {
            func: Box::new(apply_type_subst(func, subst)),
            arg: Box::new(apply_type_subst(arg, subst)),
        },
        TypeKind::Function { param, result } => TypeKind::Function {
            param: Box::new(apply_type_subst(param, subst)),
            result: Box::new(apply_type_subst(result, subst)),
        },
        TypeKind::Variant {
            constructors,
            rest,
        } => TypeKind::Variant {
            constructors: constructors
                .iter()
                .map(|c| ConstructorDef {
                    name: c.name.clone(),
                    fields: c
                        .fields
                        .iter()
                        .map(|f| Field {
                            name: f.name.clone(),
                            value: apply_type_subst(&f.value, subst),
                        })
                        .collect(),
                })
                .collect(),
            rest: rest.as_ref().and_then(|r| {
                if let Some(replacement) = subst.get(r) {
                    if let TypeKind::Var(new_name) = &replacement.node {
                        Some(new_name.clone())
                    } else {
                        None // row resolved to concrete type — close the row
                    }
                } else {
                    Some(r.clone())
                }
            }),
        },
        TypeKind::Effectful { effects, ty: inner } => TypeKind::Effectful {
            effects: effects.clone(),
            ty: Box::new(apply_type_subst(inner, subst)),
        },
        TypeKind::IO { effects, rest, ty: inner } => TypeKind::IO {
            effects: effects.clone(),
            rest: rest.clone(),
            ty: Box::new(apply_type_subst(inner, subst)),
        },
        TypeKind::Hole => TypeKind::Hole,
        TypeKind::UnitAnnotated { base, unit } => TypeKind::UnitAnnotated {
            base: Box::new(apply_type_subst(base, subst)),
            unit: unit.clone(),
        },
        TypeKind::Refined { base, predicate } => TypeKind::Refined {
            base: Box::new(apply_type_subst(base, subst)),
            predicate: predicate.clone(),
        },
        TypeKind::Forall { vars, ty: inner } => {
            // Avoid capturing bound vars: shadow them in the substitution
            // by removing entries with matching names.
            let mut inner_subst = subst.clone();
            for v in vars {
                inner_subst.remove(v);
            }
            TypeKind::Forall {
                vars: vars.clone(),
                ty: Box::new(apply_type_subst(inner, &inner_subst)),
            }
        }
    };
    Spanned::new(new_node, ty.span)
}
