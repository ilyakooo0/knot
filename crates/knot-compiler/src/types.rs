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
    /// Sources with `with history` enabled
    pub history_sources: HashSet<String>,
    /// Subset constraints: (sub, sup)
    pub subset_constraints: Vec<(RelationPath, RelationPath)>,
    /// Source refined field info: source_name -> [(field_name_or_none, refined_type_name, predicate_expr)]
    /// None field_name means the whole element type is refined.
    pub source_refinements: HashMap<String, Vec<(Option<String>, String, Expr)>>,
    /// Refined type aliases: type_name -> predicate expr
    pub refined_types: HashMap<String, Expr>,
}

impl TypeEnv {
    pub fn from_module(module: &Module) -> Self {
        let mut aliases = HashMap::new();
        let mut constructors = HashMap::new();
        let mut source_schemas = HashMap::new();
        let mut migrate_schemas: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut associated_types: HashMap<String, Vec<AssocTypeDef>> = HashMap::new();
        let mut history_sources = HashSet::new();

        let mut refined_types: HashMap<String, Expr> = HashMap::new();
        // Original AST types for aliases — used to resolve refinements through aliases
        let mut alias_ast_types: HashMap<String, Type> = HashMap::new();

        // First pass: collect type aliases and data types
        for decl in &module.decls {
            match &decl.node {
                DeclKind::TypeAlias { name, params, ty } => {
                    if params.is_empty() {
                        alias_ast_types.insert(name.clone(), ty.clone());
                        // Track refined type aliases separately
                        if let TypeKind::Refined { base, predicate } = &ty.node {
                            refined_types.insert(name.clone(), (**predicate).clone());
                            let resolved =
                                resolve_type(base, &aliases, &associated_types);
                            aliases.insert(name.clone(), resolved);
                        } else {
                            let resolved =
                                resolve_type(ty, &aliases, &associated_types);
                            aliases.insert(name.clone(), resolved);
                        }
                    }
                }
                DeclKind::Data {
                    name,
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
                                    ),
                                )
                            })
                            .collect();
                        aliases.insert(
                            name.clone(),
                            ResolvedType::Record(fields.clone()),
                        );
                        constructors.insert(ctor.name.clone(), fields);
                    } else {
                        // Multi-variant: register each constructor and create Adt alias
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
                                        ),
                                    )
                                })
                                .collect();
                            constructors.insert(ctor.name.clone(), fields.clone());
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
        // Run until stable so chained forward refs (A→B→C) all resolve
        // regardless of HashMap iteration order.
        loop {
            let mut changed = false;
            let alias_keys: Vec<String> = aliases.keys().cloned().collect();
            for name in &alias_keys {
                let resolved = aliases[name].clone();
                let fixed = re_resolve_type(&resolved, &aliases);
                if fixed != resolved {
                    aliases.insert(name.clone(), fixed);
                    changed = true;
                }
            }
            if !changed { break; }
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
                DeclKind::Source { name, ty, history } => {
                    let schema =
                        schema_for_source(ty, &aliases, &associated_types);
                    source_schemas.insert(name.clone(), schema);
                    if *history {
                        history_sources.insert(name.clone());
                    }
                    // Collect refined field info from the source type
                    let refinements = collect_source_refinements(ty, &refined_types, &alias_ast_types);
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
                    let old_resolved =
                        resolve_type(from_ty, &aliases, &associated_types);
                    let new_resolved =
                        resolve_type(to_ty, &aliases, &associated_types);
                    let old_schema = schema_descriptor(&old_resolved);
                    let new_schema = schema_descriptor(&new_resolved);
                    migrate_schemas
                        .entry(relation.clone())
                        .or_default()
                        .push((old_schema, new_schema));
                }
                DeclKind::Derived { name, ty: Some(scheme), .. } => {
                    // Compute schema for derived relations with type annotations
                    // so groupBy can use them
                    let schema =
                        schema_for_source(&scheme.ty, &aliases, &associated_types);
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
            history_sources,
            subset_constraints,
            source_refinements,
            refined_types,
        }
    }
}

/// Walk a source's type (e.g., `[{name: NonEmptyText, age: Nat}]`) and collect
/// refinement info: (field_name_or_none, refined_type_name, predicate_expr).
fn collect_source_refinements(
    ty: &Type,
    refined_types: &HashMap<String, Expr>,
    alias_ast_types: &HashMap<String, Type>,
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
        }
        // Element type is a type alias: *people : [Person]
        // Resolve through the alias to find refined fields in the underlying record.
        TypeKind::Named(name) if alias_ast_types.contains_key(name) => {
            let alias_ty = &alias_ast_types[name];
            let inner_ty = Spanned::new(TypeKind::Relation(Box::new(alias_ty.clone())), ty.span);
            result.extend(collect_source_refinements(&inner_ty, refined_types, alias_ast_types));
        }
        // Element type is a record: check each field
        TypeKind::Record { fields, .. } => {
            for field in fields {
                match &field.value.node {
                    // Field has inline refinement: age: Int where \x -> x >= 0
                    TypeKind::Refined { predicate, .. } => {
                        result.push((
                            Some(field.name.clone()),
                            field.name.clone(),
                            (**predicate).clone(),
                        ));
                    }
                    // Field references a refined type alias: age: Nat
                    TypeKind::Named(name) if refined_types.contains_key(name) => {
                        result.push((
                            Some(field.name.clone()),
                            name.clone(),
                            refined_types[name].clone(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        // Element type is a refined record: {..} where \p -> ...
        TypeKind::Refined { base, predicate } => {
            // Collect the cross-field predicate
            result.push((None, "record".into(), (**predicate).clone()));
            // Recurse into the base to collect field-level refinements
            let inner_ty = Spanned::new(TypeKind::Relation(base.clone()), ty.span);
            result.extend(collect_source_refinements(&inner_ty, refined_types, alias_ast_types));
        }
        _ => {}
    }
    result
}

fn resolve_type(
    ty: &Type,
    aliases: &HashMap<String, ResolvedType>,
    assoc_types: &HashMap<String, Vec<AssocTypeDef>>,
) -> ResolvedType {
    match &ty.node {
        TypeKind::Named(name) => match name.as_str() {
            "Int" => ResolvedType::Int,
            "Float" => ResolvedType::Float,
            "Text" => ResolvedType::Text,
            "Bool" => ResolvedType::Bool,
            "Bytes" => ResolvedType::Bytes,
            _ => aliases
                .get(name)
                .cloned()
                .unwrap_or(ResolvedType::Named(name.clone())),
        },
        TypeKind::Record { fields, .. } => {
            let resolved: Vec<(String, ResolvedType)> = fields
                .iter()
                .map(|f| {
                    (f.name.clone(), resolve_type(&f.value, aliases, assoc_types))
                })
                .collect();
            ResolvedType::Record(resolved)
        }
        TypeKind::Relation(inner) => ResolvedType::Relation(Box::new(
            resolve_type(inner, aliases, assoc_types),
        )),
        TypeKind::Function { param, result } => ResolvedType::Function(
            Box::new(resolve_type(param, aliases, assoc_types)),
            Box::new(resolve_type(result, aliases, assoc_types)),
        ),
        TypeKind::Var(_) => ResolvedType::Named("unknown".into()),
        TypeKind::App { func, arg } => {
            // Check if the function is a known associated type name
            if let TypeKind::Named(name) = &func.node {
                if let Some(defs) = assoc_types.get(name) {
                    for def in defs {
                        if !def.args.is_empty() {
                            let mut subst = HashMap::new();
                            if match_type_pattern(
                                &def.args[0],
                                arg,
                                &mut subst,
                            ) {
                                let resolved_ty =
                                    apply_type_subst(&def.ty, &subst);
                                return resolve_type(
                                    &resolved_ty,
                                    aliases,
                                    assoc_types,
                                );
                            }
                        }
                    }
                }
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
                                        &f.value, aliases, assoc_types,
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
            resolve_type(ty, aliases, assoc_types)
        }
        TypeKind::IO { ty, .. } => {
            // IO values aren't persisted — resolve inner type for diagnostics
            resolve_type(ty, aliases, assoc_types)
        }
        TypeKind::UnitAnnotated { base, .. } => {
            // Units are phantom — erase for schema resolution
            resolve_type(base, aliases, assoc_types)
        }
        TypeKind::Refined { base, .. } => {
            // Refinements are phantom for schema — base type determines storage
            resolve_type(base, aliases, assoc_types)
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

fn schema_for_source(
    ty: &Type,
    aliases: &HashMap<String, ResolvedType>,
    assoc_types: &HashMap<String, Vec<AssocTypeDef>>,
) -> String {
    // Unwrap Effectful/Refined wrappers to find the underlying type
    let unwrapped = match &ty.node {
        TypeKind::Effectful { ty: inner, .. } | TypeKind::Refined { base: inner, .. } => inner.as_ref(),
        _ => ty,
    };
    match &unwrapped.node {
        TypeKind::Relation(inner) => {
            let resolved = resolve_type(inner, aliases, assoc_types);
            schema_descriptor(&resolved)
        }
        _ => {
            // Non-relation source type (e.g. `*counter : Int`):
            // wrap as a single-column `_value` schema.
            let resolved = resolve_type(unwrapped, aliases, assoc_types);
            format!("_value:{}", col_type_str(&resolved))
        }
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
/// Nested relations are inlined as `field:[child_schema]`.
fn format_schema_field(name: &str, ty: &ResolvedType) -> String {
    if let ResolvedType::Relation(inner) = ty {
        format!("{}:[{}]", name, schema_descriptor(inner))
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
        // Type variables match anything
        (TypeKind::Var(name), _) => {
            subst.insert(name.clone(), concrete.clone());
            true
        }
        // Named types must match exactly
        (TypeKind::Named(a), TypeKind::Named(b)) => a == b,
        // Relation types recurse on the inner type
        (TypeKind::Relation(p_inner), TypeKind::Relation(c_inner)) => {
            match_type_pattern(p_inner, c_inner, subst)
        }
        // Record types match field-by-field
        (
            TypeKind::Record { fields: pf, .. },
            TypeKind::Record { fields: cf, .. },
        ) => {
            if pf.len() != cf.len() {
                return false;
            }
            pf.iter().zip(cf.iter()).all(|(p, c)| {
                p.name == c.name
                    && match_type_pattern(&p.value, &c.value, subst)
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
        TypeKind::IO { effects, ty: inner } => TypeKind::IO {
            effects: effects.clone(),
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
    };
    Spanned::new(new_node, ty.span)
}
