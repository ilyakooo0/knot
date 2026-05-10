//! Built-in trait declarations and standard implementations.
//!
//! Defines the core trait hierarchy (Eq, Ord, Num, Semigroup, Display, Functor,
//! Applicative, Monad, Alternative, Foldable, Traversable) and primitive/[]
//! implementations. Trait declarations and simple impls are parsed from Knot source;
//! complex [] impls for HKT traits (Functor, Applicative, Monad, Foldable, Traversable)
//! are registered
//! directly in codegen to avoid span collision issues.

use knot::ast;
use std::collections::HashSet;

/// Knot source for built-in trait declarations and simple implementations.
/// Complex [] impls for HKT traits are registered directly in codegen.
const PRELUDE_SOURCE: &str = r#"
trait Eq a where
  eq : a -> a -> Bool

data Ordering = LT {} | EQ {} | GT {}

trait Eq a => Ord a where
  compare : a -> a -> Ordering

trait Functor (f : Type -> Type) where
  map : (a -> b) -> f a -> f b

trait Functor f => Applicative (f : Type -> Type) where
  yield : a -> f a
  ap : f (a -> b) -> f a -> f b

trait Applicative m => Monad (m : Type -> Type) where
  bind : (a -> m b) -> m a -> m b

trait Applicative f => Alternative (f : Type -> Type) where
  empty : f a
  alt : f a -> f a -> f a

trait Foldable (t : Type -> Type) where
  fold : (b -> a -> b) -> b -> t a -> b

trait Foldable t => Traversable (t : Type -> Type) where
  traverse : (a -> f b) -> t a -> f (t b)

trait Eq a => Num a where
  add : a -> a -> a
  sub : a -> a -> a
  mul : a -> a -> a
  div : a -> a -> a
  negate : a -> a

trait Semigroup a where
  append : a -> a -> a

trait Sequence s where
  take : Int -> s -> s
  drop : Int -> s -> s

trait Display a where
  display : a -> Text

impl Display Int where
  display x = show x

impl Display Float where
  display x = show x

impl Display Text where
  display x = x

impl Display Bool where
  display x = show x

impl Alternative [] where
  empty = []
  alt a b = union a b

impl Functor Maybe where
  map f m = case m of
    Nothing {} -> Nothing {}
    Just {value} -> Just {value: f value}

impl Applicative Maybe where
  yield x = Just {value: x}
  ap fs xs = case fs of
    Nothing {} -> Nothing {}
    Just {value: f} -> case xs of
      Nothing {} -> Nothing {}
      Just {value: x} -> Just {value: f x}

impl Monad Maybe where
  bind f m = case m of
    Nothing {} -> Nothing {}
    Just {value} -> f value

impl Alternative Maybe where
  empty = Nothing {}
  alt a b = case a of
    Nothing {} -> b
    Just {} -> a

trait ToJSON a where
  toJson : a -> Text

trait FromJSON a where
  parseJson : Text -> a

min : Ord a => a -> a -> a
min = \a b -> if a < b then a else b

max : Ord a => a -> a -> a
max = \a b -> if a > b then a else b

head : [a] -> Maybe a
head = \items -> case items of
  Cons x _ -> Just {value: x}
  [] -> Nothing {}

findFirst : [a] -> (a -> Bool) -> Maybe a
findFirst = \items pred -> case items of
  Cons x rest -> if pred x then Just {value: x} else findFirst rest pred
  [] -> Nothing {}

when : Bool -> IO {| e} {} -> IO {| e} {}
when = \cond action -> if cond then action else yield {}

unless : Bool -> IO {| e} {} -> IO {| e} {}
unless = \cond action -> if cond then yield {} else action

forEach : [a] -> (a -> IO {| e} {}) -> IO {| e} {}
forEach = \items action -> case items of
  [] -> yield {}
  Cons x rest -> do
    action x
    forEach rest action
"#;

/// Parse the prelude source and prepend its declarations to the user's module.
/// Skips traits and their impls if the user already defines the same trait,
/// and skips individual prelude impls when the user already provides an impl
/// for the same `(trait, type)` pair.
pub fn inject_prelude(module: &mut ast::Module) {
    // Collect user-defined trait names to avoid conflicts
    let user_traits: HashSet<String> = module
        .decls
        .iter()
        .filter_map(|d| {
            if let ast::DeclKind::Trait { name, .. } = &d.node {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    // Collect user-provided (trait, type) impl pairs so we don't double-register
    // the same impl from the prelude (e.g. user-defined `impl Functor Maybe`).
    let user_impls: HashSet<(String, String)> = module
        .decls
        .iter()
        .filter_map(|d| {
            if let ast::DeclKind::Impl { trait_name, args, .. } = &d.node {
                let type_name = args.first().and_then(impl_arg_type_name)?;
                Some((trait_name.clone(), type_name))
            } else {
                None
            }
        })
        .collect();

    // Parse prelude
    let lexer = knot::lexer::Lexer::new(PRELUDE_SOURCE);
    let (tokens, _) = lexer.tokenize();
    let parser = knot::parser::Parser::new(PRELUDE_SOURCE.to_string(), tokens);
    let (prelude_module, _) = parser.parse_module();

    // Filter out traits/impls that the user already defines
    let filtered: Vec<ast::Decl> = prelude_module
        .decls
        .into_iter()
        .filter(|d| match &d.node {
            ast::DeclKind::Trait { name, .. } => !user_traits.contains(name),
            ast::DeclKind::Impl { trait_name, args, .. } => {
                if user_traits.contains(trait_name) {
                    return false;
                }
                match args.first().and_then(impl_arg_type_name) {
                    Some(type_name) => {
                        !user_impls.contains(&(trait_name.clone(), type_name))
                    }
                    None => true,
                }
            }
            _ => true,
        })
        .collect();

    // Prepend prelude declarations before user declarations
    let mut all_decls = filtered;
    all_decls.append(&mut module.decls);
    module.decls = all_decls;
}

/// Extract a simple type name from an impl's first type argument.
/// Mirrors `Inferencer::type_name_from_ast` so prelude filtering matches the
/// trait-resolution view of `impl Functor Maybe` ↔ ("Functor", "Maybe").
fn impl_arg_type_name(ty: &ast::Type) -> Option<String> {
    match &ty.node {
        ast::TypeKind::Named(name) => Some(name.clone()),
        ast::TypeKind::Relation(_) => Some("[]".into()),
        _ => None,
    }
}
