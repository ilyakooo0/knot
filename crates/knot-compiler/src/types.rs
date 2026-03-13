//! Minimal type resolution for schema generation.
//!
//! Resolves type aliases and computes schema descriptors that the
//! runtime uses to create SQLite tables and read/write rows.

use knot::ast::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ResolvedType {
    Int,
    Float,
    Text,
    Bool,
    Unit,
    Record(Vec<(String, ResolvedType)>),
    Relation(Box<ResolvedType>),
    Function(Box<ResolvedType>, Box<ResolvedType>),
    Named(String),
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
    /// relation_name -> (old_schema, new_schema) from `migrate` declarations
    pub migrate_schemas: HashMap<String, (String, String)>,
    /// Associated type definitions: assoc_type_name -> definitions from impls
    #[allow(dead_code)]
    pub associated_types: HashMap<String, Vec<AssocTypeDef>>,
    /// Sources with `with history` enabled
    pub history_sources: HashSet<String>,
    /// Subset constraints: (sub, sup)
    pub subset_constraints: Vec<(RelationPath, RelationPath)>,
}

impl TypeEnv {
    pub fn from_module(module: &Module) -> Self {
        let mut aliases = HashMap::new();
        let mut constructors = HashMap::new();
        let mut source_schemas = HashMap::new();
        let mut migrate_schemas = HashMap::new();
        let mut associated_types: HashMap<String, Vec<AssocTypeDef>> = HashMap::new();
        let mut history_sources = HashSet::new();

        // First pass: collect type aliases and data types
        for decl in &module.decls {
            match &decl.node {
                DeclKind::TypeAlias { name, params, ty } => {
                    if params.is_empty() {
                        let resolved =
                            resolve_type(ty, &aliases, &associated_types);
                        aliases.insert(name.clone(), resolved);
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
                        // Multi-variant: register each constructor
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
                            constructors.insert(ctor.name.clone(), fields);
                        }
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

        // Second pass: compute source schemas, migration schemas, and subset constraints
        let mut subset_constraints = Vec::new();
        for decl in &module.decls {
            match &decl.node {
                DeclKind::Source { name, ty, history } => {
                    let schema =
                        schema_for_source(ty, &aliases, &associated_types);
                    source_schemas.insert(name.clone(), schema);
                    if *history {
                        history_sources.insert(name.clone());
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
                        .insert(relation.clone(), (old_schema, new_schema));
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
        }
    }
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
        TypeKind::Variant { .. } => ResolvedType::Named("unknown".into()),
        TypeKind::Effectful { ty, .. } => {
            resolve_type(ty, aliases, assoc_types)
        }
    }
}

fn schema_for_source(
    ty: &Type,
    aliases: &HashMap<String, ResolvedType>,
    assoc_types: &HashMap<String, Vec<AssocTypeDef>>,
) -> String {
    match &ty.node {
        TypeKind::Relation(inner) => {
            let resolved = resolve_type(inner, aliases, assoc_types);
            schema_descriptor(&resolved)
        }
        _ => String::new(),
    }
}

fn schema_descriptor(ty: &ResolvedType) -> String {
    match ty {
        ResolvedType::Record(fields) => fields
            .iter()
            .map(|(name, ty)| {
                let col_type = match ty {
                    ResolvedType::Int => "int",
                    ResolvedType::Float => "float",
                    ResolvedType::Text => "text",
                    ResolvedType::Bool => "bool",
                    _ => "text",
                };
                format!("{}:{}", name, col_type)
            })
            .collect::<Vec<_>>()
            .join(","),
        _ => String::new(),
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
            rest: rest.clone(),
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
            rest: rest.clone(),
        },
        TypeKind::Effectful { effects, ty: inner } => TypeKind::Effectful {
            effects: effects.clone(),
            ty: Box::new(apply_type_subst(inner, subst)),
        },
    };
    Spanned::new(new_node, ty.span)
}
