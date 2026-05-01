//! Semantic token legend constants and the `SemanticTokensLegend` factory.
//!
//! Token-type indices are referenced by `features::semantic_tokens` and by the
//! capabilities advertisement in `main`.

use lsp_types::{SemanticTokenModifier, SemanticTokenType, SemanticTokensLegend};

pub const TOK_NAMESPACE: u32 = 0;
pub const TOK_TYPE: u32 = 1;
pub const TOK_STRUCT: u32 = 2;
pub const TOK_ENUM_MEMBER: u32 = 3;
pub const TOK_PARAMETER: u32 = 4;
pub const TOK_VARIABLE: u32 = 5;
pub const TOK_PROPERTY: u32 = 6;
pub const TOK_FUNCTION: u32 = 7;
pub const TOK_KEYWORD: u32 = 8;
pub const TOK_STRING: u32 = 9;
pub const TOK_NUMBER: u32 = 10;
pub const TOK_OPERATOR: u32 = 11;

pub const MOD_DECLARATION: u32 = 0b0001;
pub const MOD_READONLY: u32 = 0b0010;
/// Effectful operation (IO, network, fs, clock, random, console).
/// Maps to `async` since it's the closest standard token modifier — many
/// editor themes already color async calls distinctively.
pub const MOD_EFFECTFUL: u32 = 0b0100;
/// Mutation: writes to a relation (`set *r = ...`, `full set *r = ...`).
pub const MOD_MUTATION: u32 = 0b1000;

pub fn semantic_token_legend() -> SemanticTokensLegend {
    SemanticTokensLegend {
        token_types: vec![
            SemanticTokenType::NAMESPACE,    // 0
            SemanticTokenType::TYPE,         // 1
            SemanticTokenType::STRUCT,       // 2
            SemanticTokenType::ENUM_MEMBER,  // 3
            SemanticTokenType::PARAMETER,    // 4
            SemanticTokenType::VARIABLE,     // 5
            SemanticTokenType::PROPERTY,     // 6
            SemanticTokenType::FUNCTION,     // 7
            SemanticTokenType::KEYWORD,      // 8
            SemanticTokenType::STRING,       // 9
            SemanticTokenType::NUMBER,       // 10
            SemanticTokenType::OPERATOR,     // 11
        ],
        token_modifiers: vec![
            SemanticTokenModifier::DECLARATION,  // bit 0
            SemanticTokenModifier::READONLY,     // bit 1
            SemanticTokenModifier::ASYNC,        // bit 2 — used for effectful calls
            SemanticTokenModifier::MODIFICATION, // bit 3 — used for relation mutations
        ],
    }
}
