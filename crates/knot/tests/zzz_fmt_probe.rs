// TEMPORARY audit scratch. Delete after use.
use knot::diagnostic::Severity;
use knot::format::format_module;
use knot::lexer::Lexer;
use knot::parser::Parser;

fn parse(src: &str) -> Option<knot::ast::Module> {
    let (tokens, ld) = Lexer::new(src).tokenize();
    if ld.iter().any(|d| d.severity == Severity::Error) {
        return None;
    }
    let (m, pd) = Parser::new(src.to_string(), tokens).parse_module();
    if pd.iter().any(|d| d.severity == Severity::Error) {
        return None;
    }
    Some(m)
}

fn fmt(src: &str) -> Result<String, String> {
    let m = match parse(src) {
        Some(m) => m,
        None => return Err("PARSE-ERROR".into()),
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| format_module(src, &m))) {
        Ok(o) => Ok(o),
        Err(_) => Err("PANIC".into()),
    }
}

/// Strip spans from a debug AST rendering (crude — good enough to compare).
fn ast_dbg(src: &str) -> Option<String> {
    let m = parse(src)?;
    let s = format!("{:?}", m);
    // remove Span { start: N, end: N }
    let mut out = String::new();
    let mut rest = s.as_str();
    while let Some(idx) = rest.find("Span { start: ") {
        out.push_str(&rest[..idx]);
        rest = &rest[idx..];
        if let Some(end) = rest.find(" }") {
            rest = &rest[end + 2..];
        } else {
            break;
        }
    }
    out.push_str(rest);
    Some(out)
}

#[test]
fn probe_all() {
    // (name, messy-but-valid source). We check: does formatting change it
    // (else likely a safety-net revert = printer bug), is it idempotent, does
    // formatted output reparse to the SAME ast as input (semantic preservation).
    let cases: &[(&str, &str)] = &[
        ("int-lit", "x   =    42"),
        ("float-whole", "x = 3.0"),
        ("float-frac", "x = 3.14"),
        ("float-neg", "x = -3.0"),
        ("neg-int", "x = -5"),
        ("neg-neg", "x = - -5"),
        ("text-esc", "x = \"a\\tb\\nc\""),
        ("text-quote", "x = \"say \\\"hi\\\"\""),
        ("bytes", "x = b\"\\x00\\xff AB\""),
        ("bool", "x = true"),
        ("record", "x = {  a : 1 ,b:2 }"),
        ("record-pun", "x = {a: a, b: b}"),
        ("record-empty", "x = {}"),
        ("record-update", "x = {r | a: 1, b: 2}"),
        ("field-access", "x = a.b.c"),
        ("list", "x = [ 1 ,2, 3 ]"),
        ("list-empty", "x = []"),
        ("lambda", "x = \\a b -> a"),
        ("app", "x = f    a   b"),
        ("binop", "x = 1+2*3"),
        ("binop-sub-assoc", "x = 10 - (5 - 2)"),
        ("binop-concat", "x = (a ++ b) ++ c"),
        ("pipe", "x = xs |> filter f |> map g"),
        ("unary-not", "x = not y"),
        ("if", "x = if a then b else c"),
        ("case", "x = case y of\n  A {} -> 1\n  B {} -> 2"),
        ("do", "x = do\n  a <- f\n  yield a"),
        ("set", "main = *r = [1]"),
        ("replace", "main = replace *r = [1]"),
        ("atomic", "main = atomic (*r = [1])"),
        ("unitlit-upper", "x = 42.0 M"),
        ("unitlit-lower", "x = 999 usd"),
        ("unitlit-lower-arg", "x = f 999 usd"),
        ("timeunit", "main = sleep (2 seconds)"),
        ("annot", "x = (y : Int)"),
        ("annot-lambda", "x = (\\a -> a : Int -> Int)"),
        ("refine", "x = refine y"),
        ("data-1", "data Box a = Box {value: a}"),
        ("data-2", "data B = T {} | F {}"),
        ("data-deriv", "data C = A {} | B {} deriving (Eq, Ord)"),
        ("type-alias", "type P = {name: Text, age: Int}"),
        ("type-fn", "type F = Int -> Int -> Bool"),
        ("type-forall", "f : forall a. a -> a\nf = \\x -> x"),
        ("type-io", "f : IO {console} {}\nf = println \"x\""),
        ("type-io-rest", "f : IO {console | r} Int\nf = f"),
        ("type-refined", "type Nat = Int where \\x -> x >= 0"),
        ("type-variant", "type S = <Open {} | Closed {by: Text}>"),
        ("type-unit", "f : Float (m / s^2)\nf = f"),
        ("effectful", "type E = {rw *people} Text -> {}"),
        ("source", "*people : [Person]"),
        ("view", "*v = do\n  p <- *people\n  yield p"),
        ("derived", "&d = *people"),
        ("trait", "trait Eq a where\n  eq : a -> a -> Bool"),
        ("trait-default", "trait Show a where\n  show : a -> Text\n  show x = \"?\""),
        ("trait-super", "trait Ord a where\n  cmp : a -> a -> Int"),
        ("impl", "impl Eq Int where\n  eq a b = true"),
        ("impl-assoc", "impl Container Box where\n  type Elem Box = Int"),
        ("route", "route Api where\n  GET /todos -> [Todo]"),
        ("route-body", "route Api where\n  POST {name: Text} /todos -> Todo"),
        ("route-composite", "route All = A | B"),
        ("migrate", "migrate *r from Int to Text using f"),
        ("subset", "*orders.customer <= *people.name"),
        ("unit-decl", "unit m"),
        ("unit-def", "unit N = kg * m / s^2"),
        ("cons-pat", "f = \\xs -> case xs of\n  Cons h t -> h\n  [] -> 0"),
        ("cons-ctor-empty", "f = \\x -> case x of\n  Cons -> 1\n  _ -> 0"),
        ("list-pat", "f = \\x -> case x of\n  [a, b] -> a\n  _ -> 0"),
        ("groupby", "x = do\n  o <- *orders\n  groupBy o.customer\n  yield o"),
        ("serve", "s = serve Api where\n  Get = \\r -> Ok {value: 1}"),
        ("nested-relation", "*people : [{name: Text, tags: [Tag]}]"),
        ("let-in", "x = let a = 1 in a + 1"),
        ("let-in-annot", "x = let a : Int = 1 in a"),
        ("annot-unit", "x = (5 : Int m)"),
        ("mod", "x = 10 % 3"),
        ("multi-bound", "f : Eq a => Ord a => a -> a\nf = \\x -> x"),
        ("hkt-trait", "trait Functor (f : Type -> Type) where\n  map : (a -> b) -> f a -> f b"),
        ("route-headers", "route Api where\n  GET /x headers {authorization: Text} -> Res"),
        ("route-resp-headers", "route Api where\n  GET /x -> Res headers {contentType: Text}"),
        ("route-query", "route Api where\n  GET /x?{limit: Int} -> Res"),
        ("route-ratelimit", "route Api where\n  GET /x -> Res rateLimit rl"),
        ("import", "import ./foo (a, B)"),
        ("import-plain", "import ./foo"),
        ("export", "export type A = Int"),
        ("time-unit-ms", "main = sleep (5 ms)"),
        ("negate-field", "x = -a.b"),
        ("app-neg-arg", "x = f (-5)"),
        ("app-neg-arg2", "x = f -5"),
        ("deeply-nested-app", "x = f (g (h (i j)))"),
        ("string-with-dashes", "x = \"a -- b\""),
        ("record-in-list", "x = [{a: 1}, {b: 2}]"),
        ("case-in-list", "x = [case y of\n  A {} -> 1]"),
        ("float-exp", "x = 1000000000000000000000.0"),
        ("float-small", "x = 0.0001"),
    ];

    for (name, src) in cases {
        let out = match fmt(src) {
            Ok(o) => o,
            Err(e) => {
                println!("!! {name}: {e}\n   src={src:?}");
                continue;
            }
        };
        let changed = &out != src;
        // idempotency
        let out2 = fmt(&out).unwrap_or_else(|e| format!("<{e}>"));
        let idem = out2 == out;
        // semantic preservation: reparse formatted == reparse src
        let same_ast = match (ast_dbg(src), ast_dbg(&out)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        };
        let mut flags = Vec::new();
        if !changed {
            flags.push("UNCHANGED(possible-revert)");
        }
        if !idem {
            flags.push("NON-IDEMPOTENT");
        }
        if !same_ast {
            flags.push("AST-DIFF(reverted-or-semantic)");
        }
        if !flags.is_empty() {
            println!(
                "?? {name}: {}\n   --in--\n{src}\n   --out--\n{out}\n   --out2--\n{out2}\n",
                flags.join(",")
            );
        }
    }
}
