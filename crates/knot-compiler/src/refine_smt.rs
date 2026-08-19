//! Compile-time subtyping between refined types via SMT.
//!
//! A refined value of type `S` may flow where a *looser* refined type `T` is
//! required — without an explicit `refine` — when `S`'s predicate provably
//! implies `T`'s predicate over a shared base type:
//!
//! ```text
//!     S <: T   iff   base(S) == base(T)  ∧  ∀x. P_S(x) ⟹ P_T(x)
//! ```
//!
//! The implication is discharged by Z3 as the unsatisfiability of
//! `P_S(x) ∧ ¬P_T(x)`. Only a decidable fragment of predicates is encoded
//! (integer/real arithmetic, comparisons, `&&`/`||`/`!`, `==`/`!=`); anything
//! outside that fragment — strings, function calls, ADTs, relations — fails to
//! translate and the caller falls back to requiring `refine`. A solver timeout
//! or `Unknown` result is likewise treated as "cannot prove", never as
//! permission: we only widen when Z3 proves `Unsat`.

use knot::ast;
use z3::ast::{Bool, Dynamic, Int, Real};
use z3::{SatResult, Solver};

/// The resolved, non-refined base kind a refined type is built over. The
/// caller (inference, which owns the private `Ty`) classifies the base and
/// checks that the two types share it before calling [`implies`] — this module
/// only deals with the numeric fragment Z3 can reason about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BaseKind {
    Int,
    Real,
}

/// Prove that a value of refined type `source` can be used wherever refined
/// type `target` is required: `∀x. (∧ P_source) ⟹ (∧ P_target)`.
///
/// Each slice is the predicate chain of one refined type (its own predicate
/// plus every link in its declared base chain); the effective predicate is
/// their conjunction. Both types must share the numeric base `base` (the
/// caller checks). Returns `true` only when Z3 proves the implication valid;
/// `false` means "cannot prove" (not a subtype, an un-encodable predicate, or
/// a timeout) — in every such case the caller must keep requiring `refine`.
pub fn implies(source_preds: &[ast::Expr], target_preds: &[ast::Expr], base: BaseKind) -> bool {
    let is_real = base == BaseKind::Real;
    let src = translate_all(source_preds, is_real);
    let tgt = translate_all(target_preds, is_real);
    let (src, tgt) = match (src, tgt) {
        (Some(s), Some(t)) => (s, t),
        _ => return false, // a predicate outside the decidable fragment
    };
    let solver = Solver::new();
    let mut params = z3::Params::new();
    params.set_u32("timeout", 2000);
    solver.set_params(&params);
    for f in &src {
        solver.assert(f);
    }
    // ∧P_S ∧ ¬(∧P_T) unsat  ⟺  ∧P_S ⟹ ∧P_T valid.  ¬(∧P_T) = ∨(¬P_T_i).
    let neg_tgt: Vec<Bool> = tgt.iter().map(|f| f.not()).collect();
    let neg_tgt_refs: Vec<&Bool> = neg_tgt.iter().collect();
    solver.assert(Bool::or(&neg_tgt_refs));
    solver.check() == SatResult::Unsat
}

/// Translate a conjunction of predicate lambdas into Z3 formulas over one
/// shared constant. Each predicate binds its own parameter name; we substitute
/// every one of them with the same constant so the formulas range over a
/// single value. Returns `None` if any predicate is outside the fragment.
fn translate_all(preds: &[ast::Expr], is_real: bool) -> Option<Vec<Bool>> {
    let var: Dynamic = if is_real {
        Real::new_const("x").into()
    } else {
        Int::new_const("x").into()
    };
    preds
        .iter()
        .map(|p| translate_one(p, &var, is_real))
        .collect()
}

/// Translate `\param -> body` into a Z3 formula over the shared constant `var`
/// (substituted for `param`). The body must be in the decidable fragment or
/// the whole translation returns `None`.
fn translate_one(pred: &ast::Expr, var: &Dynamic, is_real: bool) -> Option<Bool> {
    let (param, body) = match &pred.node {
        ast::ExprKind::Lambda { params, body, .. } if params.len() == 1 => {
            let name = match &params[0].node {
                ast::PatKind::Var(n) => n.clone(),
                _ => return None,
            };
            (name, body)
        }
        _ => return None,
    };
    bool_expr(body, &param, var, is_real)
}

/// Translate a boolean-position expression (the predicate body, or an operand
/// of `&&`/`||`/`!`/comparison).
fn bool_expr(e: &ast::Expr, param: &str, var: &Dynamic, is_real: bool) -> Option<Bool> {
    match &e.node {
        // `Bool.True {}` / `Bool.False {}` — Bool's constructors are the
        // user-facing boolean constants (Bool is a compiler-special primitive).
        ast::ExprKind::Constructor(name) if name == "True" || name == "False" => {
            Some(Bool::from_bool(name == "True"))
        }
        ast::ExprKind::Annot { expr, .. } => bool_expr(expr, param, var, is_real),
        ast::ExprKind::UnaryOp {
            op: ast::UnaryOp::Not,
            operand,
            ..
        } => Some(bool_expr(operand, param, var, is_real)?.not()),
        ast::ExprKind::BinOp { op, lhs, rhs, .. } => match op {
            ast::BinOp::And => Some(Bool::and(&[
                &bool_expr(lhs, param, var, is_real)?,
                &bool_expr(rhs, param, var, is_real)?,
            ])),
            ast::BinOp::Or => Some(Bool::or(&[
                &bool_expr(lhs, param, var, is_real)?,
                &bool_expr(rhs, param, var, is_real)?,
            ])),
            // Comparisons and (in)equality over arithmetic operands.
            ast::BinOp::Lt
            | ast::BinOp::Gt
            | ast::BinOp::Le
            | ast::BinOp::Ge
            | ast::BinOp::Eq
            | ast::BinOp::Neq => {
                let l = arith_expr(lhs, param, var, is_real)?;
                let r = arith_expr(rhs, param, var, is_real)?;
                cmp_expr(*op, &l, &r, is_real)
            }
            _ => None,
        },
        _ => None,
    }
}

/// Build a comparison/equality between two arithmetic values of the right sort.
fn cmp_expr(op: ast::BinOp, l: &Dynamic, r: &Dynamic, is_real: bool) -> Option<Bool> {
    if is_real {
        let l = l.as_real()?;
        let r = r.as_real()?;
        Some(match op {
            ast::BinOp::Lt => l.lt(&r),
            ast::BinOp::Gt => l.gt(&r),
            ast::BinOp::Le => l.le(&r),
            ast::BinOp::Ge => l.ge(&r),
            ast::BinOp::Eq => l.eq(&r),
            ast::BinOp::Neq => l.eq(&r).not(),
            _ => return None,
        })
    } else {
        let l = l.as_int()?;
        let r = r.as_int()?;
        Some(match op {
            ast::BinOp::Lt => l.lt(&r),
            ast::BinOp::Gt => l.gt(&r),
            ast::BinOp::Le => l.le(&r),
            ast::BinOp::Ge => l.ge(&r),
            ast::BinOp::Eq => l.eq(&r),
            ast::BinOp::Neq => l.eq(&r).not(),
            _ => return None,
        })
    }
}

/// Translate an arithmetic-position expression (operand of a comparison or of
/// `+ - * / %`). Only the predicate parameter, numeric literals, and arithmetic
/// operators are encodable; anything else returns `None`.
fn arith_expr(e: &ast::Expr, param: &str, var: &Dynamic, is_real: bool) -> Option<Dynamic> {
    match &e.node {
        ast::ExprKind::Var(name) if name == param => Some(var.clone()),
        ast::ExprKind::Lit(ast::Literal::Int(s)) => {
            let n: i64 = s.parse().ok()?;
            Some(if is_real {
                // An integer literal in a real context is exactly n/1.
                Real::from_rational_str(&n.to_string(), "1")?.into()
            } else {
                Int::from_i64(n).into()
            })
        }
        ast::ExprKind::Lit(ast::Literal::Float(f)) => {
            // Encode as an exact rational when possible (decimal floats with a
            // short expansion); otherwise bail rather than lose precision.
            Some(real_from_f64(*f)?.into())
        }
        ast::ExprKind::Annot { expr, .. } => arith_expr(expr, param, var, is_real),
        ast::ExprKind::UnaryOp {
            op: ast::UnaryOp::Neg,
            operand,
            ..
        } => {
            let v = arith_expr(operand, param, var, is_real)?;
            if is_real {
                Some(v.as_real()?.unary_minus().into())
            } else {
                Some(v.as_int()?.unary_minus().into())
            }
        }
        ast::ExprKind::BinOp { op, lhs, rhs, .. } => {
            let l = arith_expr(lhs, param, var, is_real)?;
            let r = arith_expr(rhs, param, var, is_real)?;
            if is_real {
                let l = l.as_real()?;
                let r = r.as_real()?;
                Some(match op {
                    ast::BinOp::Add => (l + r).into(),
                    ast::BinOp::Sub => (l - r).into(),
                    ast::BinOp::Mul => (l * r).into(),
                    ast::BinOp::Div => (l / r).into(),
                    _ => return None,
                })
            } else {
                let l = l.as_int()?;
                let r = r.as_int()?;
                Some(match op {
                    ast::BinOp::Add => (l + r).into(),
                    ast::BinOp::Sub => (l - r).into(),
                    ast::BinOp::Mul => (l * r).into(),
                    ast::BinOp::Div => l.div(r).into(),
                    ast::BinOp::Mod => l.rem(r).into(),
                    _ => return None,
                })
            }
        }
        _ => None,
    }
}

/// Encode an `f64` as an exact Z3 rational. We recover the float's exact
/// binary value (`mantissa * 2^exp`) and hand Z3 the numerator/denominator as
/// decimal strings so `x <= 100.0` means exactly 100.
fn real_from_f64(f: f64) -> Option<Real> {
    if !f.is_finite() {
        return None;
    }
    let bits = f.to_bits();
    let sign = if bits >> 63 == 1 { -1i128 } else { 1i128 };
    let raw_exp = ((bits >> 52) & 0x7ff) as i64;
    let frac = (bits & 0xfffffffffffff) as i128;
    let (mantissa, exp2) = if raw_exp == 0 {
        (frac, -1074i64) // subnormal
    } else {
        (frac | (1 << 52), raw_exp - 1075)
    };
    let mantissa = mantissa * sign;
    // mantissa * 2^exp2 as num/den.
    let (num, den) = if exp2 >= 0 {
        (mantissa.checked_shl(exp2.min(120) as u32)?, 1i128)
    } else {
        (mantissa, 1i128.checked_shl((-exp2).min(120) as u32)?)
    };
    Real::from_rational_str(&num.to_string(), &den.to_string())
}
