//! Minimal type resolution for schema generation.
//!
//! Resolves type aliases and computes schema descriptors that the
//! runtime uses to create SQLite tables and read/write rows.

use knot::ast::*;
use std::collections::HashMap;

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

pub struct TypeEnv {
    #[allow(dead_code)]
    pub aliases: HashMap<String, ResolvedType>,
    /// constructor_name -> Vec<(field_name, field_type)>
    pub constructors: HashMap<String, Vec<(String, ResolvedType)>>,
    /// source_name -> schema descriptor string ("col:type,col:type,...")
    pub source_schemas: HashMap<String, String>,
}

impl TypeEnv {
    pub fn from_module(module: &Module) -> Self {
        let mut aliases = HashMap::new();
        let mut constructors = HashMap::new();
        let mut source_schemas = HashMap::new();

        // First pass: collect type aliases and data types
        for decl in &module.decls {
            match &decl.node {
                DeclKind::TypeAlias { name, params, ty } => {
                    if params.is_empty() {
                        let resolved = resolve_type(ty, &aliases);
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
                            .map(|f| (f.name.clone(), resolve_type(&f.value, &aliases)))
                            .collect();
                        aliases.insert(name.clone(), ResolvedType::Record(fields.clone()));
                        constructors.insert(ctor.name.clone(), fields);
                    } else {
                        // Multi-variant: register each constructor
                        for ctor in ctors {
                            let fields: Vec<(String, ResolvedType)> = ctor
                                .fields
                                .iter()
                                .map(|f| (f.name.clone(), resolve_type(&f.value, &aliases)))
                                .collect();
                            constructors.insert(ctor.name.clone(), fields);
                        }
                    }
                }
                _ => {}
            }
        }

        // Second pass: compute source schemas
        for decl in &module.decls {
            if let DeclKind::Source { name, ty, .. } = &decl.node {
                let schema = schema_for_source(ty, &aliases);
                source_schemas.insert(name.clone(), schema);
            }
        }

        Self {
            aliases,
            constructors,
            source_schemas,
        }
    }
}

fn resolve_type(ty: &Type, aliases: &HashMap<String, ResolvedType>) -> ResolvedType {
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
                .map(|f| (f.name.clone(), resolve_type(&f.value, aliases)))
                .collect();
            ResolvedType::Record(resolved)
        }
        TypeKind::Relation(inner) => {
            ResolvedType::Relation(Box::new(resolve_type(inner, aliases)))
        }
        TypeKind::Function { param, result } => ResolvedType::Function(
            Box::new(resolve_type(param, aliases)),
            Box::new(resolve_type(result, aliases)),
        ),
        TypeKind::Var(_) => ResolvedType::Named("unknown".into()),
        TypeKind::App { .. } => ResolvedType::Named("unknown".into()),
        TypeKind::Variant { .. } => ResolvedType::Named("unknown".into()),
        TypeKind::Effectful { ty, .. } => resolve_type(ty, aliases),
    }
}

fn schema_for_source(ty: &Type, aliases: &HashMap<String, ResolvedType>) -> String {
    match &ty.node {
        TypeKind::Relation(inner) => {
            let resolved = resolve_type(inner, aliases);
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
