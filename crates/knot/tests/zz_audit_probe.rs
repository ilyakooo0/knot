use knot::lexer::Lexer;
use knot::parser::Parser;
use knot::format::format_module;
use knot::diagnostic::Severity;

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

// Feed messy input. If output == input, the formatter silently REVERTED
// (round-trip failure) because the messy input would obviously be reformatted.
fn probe_revert(label: &str, messy: &str) {
    let Some(m) = parse(messy) else {
        println!("[{}] INPUT DOES NOT PARSE", label);
        return;
    };
    let out = format_module(messy, &m);
    if out == messy {
        println!("=== [{}] !!! SILENT REVERT (round-trip failed) !!!\ninput:\n{}\n", label, messy);
    } else {
        // did it reparse & is it idempotent?
        let ok = parse(&out).is_some();
        let idem = parse(&out).map(|m2| format_module(&out, &m2) == out).unwrap_or(false);
        println!("=== [{}] formatted ok, parses={} idem={} ===\n{}", label, ok, idem, out);
    }
}

#[test]
fn audit4() {
    // Messy versions that MUST reformat (if they revert -> round-trip bug)
    probe_revert("messy_do", "main=do\n    x<-foo\n    yield    x\n");
    probe_revert("messy_case", "f=\\x->case x of\n  A{}->1\n  B{}->2\n");
    probe_revert("messy_record", "main={a:1,b:2,c:3}\n");
    probe_revert("messy_nested_do_case", "main=do\n x<-foo\n case x of\n  Just{value:v}->do\n   println v\n   yield v\n  Nothing{}->yield 0\n");
    probe_revert("messy_if", "main=if   a then   b   else c\n");
    probe_revert("messy_with", "main=with {x:1} with {y:2} x+y\n");
    probe_revert("messy_lambda", "f=\\x    y->x+y\n");
    probe_revert("messy_trait", "trait Eq a where\n eq:a->a->Bool\n neq x y=not (eq x y)\n");
    probe_revert("messy_impl", "impl Eq Int where\n eq x y=x==y\n");
    probe_revert("messy_route", "route Api where\n GET /u/{id:Int}->User=GetU\n POST {name:Text} /u->User=MakeU\n");
    probe_revert("messy_data", "data Tree a=Leaf{}|Node{left:Tree a,value:a,right:Tree a}\n");
    probe_revert("messy_type_alias", "type   T={x:Int,y:Int}\n");
    probe_revert("messy_nested_records", "main={a:{b:{c:{d:1}}}}\n");
    probe_revert("messy_view", "*active=do\n u<-*users\n where u.active\n yield u\n");
    probe_revert("messy_pipe", "main=xs|>filter f|>map g\n");
    probe_revert("messy_binop_parens", "main=(a+b)*(c+d)\n");
    probe_revert("messy_deep_app", "main=f a b c d e\n");
    probe_revert("messy_annot", "main=x:Int\n");
    probe_revert("messy_case_arms_indent", "f=\\x->case x of A{}->1;B{}->2;C{}->3\n");
    probe_revert("messy_do_semicolons", "main=do x<-a;y<-b;yield (x+y)\n");
    probe_revert("messy_serve", "s=serve Api where GetU=handler1;MakeU=handler2\n");
    probe_revert("messy_effectful_type", "f:{console} Int->{console} Int\nf=\\x->x\n");
    probe_revert("messy_io_type", "f:Int->IO {console} {}\nf=\\x->println x\n");
    probe_revert("messy_refined", "type Nat=Int where \\x->x>=0\n");
    probe_revert("messy_units", "speed:Float (M / S)\nspeed=1.0 (M / S)\n");
    probe_revert("messy_large_signature", "process:Aaaaaaaaaa->Bbbbbbbbbb->Cccccccccc->Dddddddddd->Eeeeeeeeee->Ffffffffff->Gggggggggg\nprocess=undefined\n");
}
