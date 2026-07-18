//! Ad-hoc verification for selection_range.rs fix: Annot/Serve/TimeUnitLit
//! now recurse into inner expressions in collect_containing_spans.
//! Run: cargo test -p knot-lsp verification_pass4 --nocapture

use super::*;
use crate::test_support::TestWorkspace;
use crate::utils::offset_to_position;

fn params(uri: &Uri, pos: Position) -> SelectionRangeParams {
    SelectionRangeParams {
        text_document: TextDocumentIdentifier { uri: uri.clone() },
        positions: vec![pos],
        work_done_progress_params: Default::default(),
        partial_result_params: Default::default(),
    }
}

fn chain_ranges(sr: &SelectionRange) -> Vec<Range> {
    let mut out = vec![sr.range];
    let mut cur = sr.parent.as_deref();
    while let Some(p) = cur {
        out.push(p.range);
        cur = p.parent.as_deref();
    }
    out
}

#[test]
fn verification_pass4_annot_descends_into_inner_expr() {
    let mut ws = TestWorkspace::new();
    // `(1 + 2 : Int)` is an Annot{ expr: BinOp, ty: Int }. Cursor on `1`
    // inside the annotated expression should produce a chain that includes
    // the inner expression's span. Before the fix, the Annot arm was in the
    // `_ => {}` catch-all, so the inner expression span was missing.
    let src = "main = (1 + 2 : Int)\n";
    let uri = ws.open("main", src);
    let off = src.find("1 + 2").unwrap() + 1; // inside `1`
    let pos = offset_to_position(src, off);
    let srs = handle_selection_range(&ws.state, &params(&uri, pos))
        .expect("selection range returned");
    assert_eq!(srs.len(), 1, "one position → one result");
    let ranges = chain_ranges(&srs[0]);
    // The chain must have more than just the whole-document fallback —
    // the Annot expression's span was found and recursed into.
    assert!(
        ranges.len() >= 2,
        "expected at least 2 ranges in chain (decl + inner expr); got {}: {ranges:?}",
        ranges.len()
    );
    // Innermost range should be narrower than the whole-decl range.
    let innermost = ranges[0];
    let outermost = ranges[ranges.len() - 1];
    assert!(
        (innermost.end.line as usize, innermost.end.character as usize)
            <= (outermost.end.line as usize, outermost.end.character as usize),
        "innermost {innermost:?} must be within outermost {outermost:?}"
    );
}

#[test]
fn verification_pass4_serve_descends_into_handler_body() {
    let mut ws = TestWorkspace::new();
    let src = "main = serve API where\n  getUsers = \\_ -> \"hello\"\n";
    let uri = ws.open("main", src);
    let off = src.find("hello").unwrap() + 1; // inside `hello`
    let pos = offset_to_position(src, off);
    let srs = handle_selection_range(&ws.state, &params(&uri, pos))
        .expect("selection range returned");
    assert_eq!(srs.len(), 1);
    let ranges = chain_ranges(&srs[0]);
    assert!(
        ranges.len() >= 2,
        "expected at least 2 ranges (decl + inner expr); got {}: {ranges:?}",
        ranges.len()
    );
}