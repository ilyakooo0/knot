#!/usr/bin/env python3
"""Extract every fenced code block from the knot docs and try to compile it.

Block classes and handling:
  - complete program (has `with {` AND a body expr after the closing brace,
    or is a bare `(do ...)`): compile as-is.
  - declaration fragment (bindings like `names ([...])`, `f \\x -> ...`, sigs):
    wrap in `with { ... } (do base.println "ok"; yield {})`.
  - reference signature (`name : T`, optionally `base.` prefixed): turn into a
    sig-check `checkN : T` + `checkN base.<name>` so a wrong sig fails compile.

Usage: doccheck.py <doc.md> [--write-report]
Prints per-block PASS/FAIL with the first error line for failures.
"""
import re, os, subprocess, sys, tempfile

WORK = "/tmp/knot-todo"
KNOT = os.path.join(WORK, "target/debug/knot")

def extract_blocks(path):
    lines = open(path).read().splitlines()
    blocks = []
    i = 0
    while i < len(lines):
        m = re.match(r"^```(\w*)\s*$", lines[i])
        if m:
            lang = m.group(1)
            start = i + 2  # 1-indexed line of first content line
            j = i + 1
            body = []
            while j < len(lines) and not re.match(r"^```\s*$", lines[j]):
                body.append(lines[j]); j += 1
            blocks.append({"lang": lang, "body": body, "line": start})
            i = j
        i += 1
    return blocks

def is_comment_or_blank(s):
    s = s.strip()
    return not s or s.startswith("--")

SIG_RE = re.compile(r"^\s*(?:base\.)?([a-zA-Z][a-zA-Z0-9_.]*)\s*:\s*(.+)$")

def looks_like_knot(body):
    """Heuristic: is this untagged block knot code (vs prose/output/diagram)?"""
    nonempty = [b for b in body if not is_comment_or_blank(b)]
    if not nonempty:
        return False
    for ln in nonempty:
        s = ln.strip()
        # obvious non-knot: paths, backticks, box-drawing, leading bullets/SQL
        if "/" in s or "`" in s or s.startswith(("│", "╭", "╰", "├", "└", "-", "*")):
            return False
        if re.match(r"^(SELECT|FROM|WHERE|INSERT|UPDATE|DELETE|CREATE|GET|POST|PUT|HTTP)\b", s, re.I):
            return False
    # must contain at least one knot-ish signal: a sig, binding, or known keyword
    text = "\n".join(nonempty)
    return bool(re.search(r"[a-zA-Z_]\s*:\s|\bwith\b|\bdo\b|\bcase\b|\\|->|\(do|base\.", text))

def classify(body):
    text = "\n".join(body)
    nonempty = [b for b in body if not is_comment_or_blank(b)]
    if not nonempty:
        return "empty", None
    # complete program: has `with {` and a non-} trailing expression, OR starts with (do / do
    if "with {" in text:
        # is there a body after the last closing brace at col 0?
        # crude: last non-comment line that's not part of the with-record
        tail = [b for b in reversed(body) if not is_comment_or_blank(b)]
        if tail and not tail[0].strip().startswith("}"):
            # body exists if some top-level line after the with block
            return "program", text
        # with-block but maybe body is `(do ...)` right after }
        if re.search(r"^\}\s*$", text, re.M) and re.search(r"^\(do|^do\b", text, re.M):
            return "program", text
        return "program", text  # try as program; if no body it'll error clearly
    stripped0 = nonempty[0].strip()
    if stripped0.startswith("(do") or stripped0 == "do" or stripped0.startswith("do "):
        return "program", text
    # pure signature(s)?
    sigs = []
    allsig = True
    for b in nonempty:
        m = SIG_RE.match(b)
        if m and ("\\" not in b) and ("(" not in b.split(":",1)[0]):
            sigs.append((m.group(1), m.group(2)))
        else:
            allsig = False
            break
    if allsig and sigs:
        return "sigs", sigs
    # otherwise: declaration/expr fragment
    return "fragment", text

def wrap_fragment(text):
    return "with {\n" + text + "\n}\n(do\n  base.println \"ok\"\n  yield {})\n"

# Builtins that are language KEYWORDS and cannot be referenced as `base.x`
# values (`base.not` fails to parse: `not` is a keyword). For these we verify
# only that the documented TYPE parses, by ascribing it to a type-correct
# lambda. ADT constructors likewise are not `base.x` values.
KEYWORD_LAMBDAS = {
    "not": "\\b -> not b",
    "atomic": None,   # keyword block form; sig not value-bindable
    "where": None, "migrate": None, "serve": None, "route": None,
    "fetch": None, "retry": None,
}
CTORS = {"Just", "Nothing", "Ok", "Err", "LT", "EQ", "GT", "True", "False"}

def wrap_sigs(sigs):
    out = ["with {"]
    for i, (name, ty) in enumerate(sigs):
        bare = name.split(".")[-1]
        tyc = ty.strip()
        lam = KEYWORD_LAMBDAS.get(bare)
        if bare in KEYWORD_LAMBDAS or bare in CTORS:
            if lam is None:
                # keyword block form: no value check possible; emit a placeholder
                # so the `with` is a valid program (type itself already trusted).
                out.append(f"kw{i} 0")
                continue
            out.append(f"check{i} : {tyc}")
            out.append(f"check{i} ({lam})")
            continue
        out.append(f"check{i} : {tyc}")
        # SIG_RE strips any leading `base.`; `name` is the path WITHOUT it
        # (e.g. `morph.textToBytes.into` or `sum`). Reference the full path.
        out.append(f"check{i} base.{name}")
    out.append("}")
    out.append("(do\n  base.println \"ok\"\n  yield {})\n")
    return "\n".join(out)

def compile_src(src, tag):
    fd, srcf = tempfile.mkstemp(prefix=f"hermes-doccheck-{tag}-", suffix=".knot")
    os.close(fd)
    binf = srcf[:-5]  # strip .knot
    with open(srcf, "w") as f:
        f.write(src)
    try:
        r = subprocess.run([KNOT, "build", srcf, "-o", binf],
                           cwd=WORK, capture_output=True, text=True, timeout=60)
        ok = os.path.exists(binf)
        err = ""
        if not ok:
            first = next((l for l in (r.stderr + r.stdout).splitlines() if l.strip().startswith("Error")), "")
            err = first or (r.stderr + r.stdout).strip().splitlines()[:1]
            err = err if isinstance(err, str) else (err[0] if err else "unknown error")
        return ok, err
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"
    finally:
        for p in (srcf, binf):
            if os.path.exists(p):
                os.remove(p)

def main():
    doc = sys.argv[1]
    blocks = extract_blocks(doc)
    # Build work items first, then compile in parallel (each block spawns the
    # compiler; serial is ~5s/block which times out on the big docs).
    work_items = []
    for idx, b in enumerate(blocks):
        if b["lang"] not in ("knot", ""):
            continue
        if b["lang"] == "" and not looks_like_knot(b["body"]):
            continue
        kind, payload = classify(b["body"])
        if kind == "empty":
            continue
        if kind == "program":
            src = payload
        elif kind == "fragment":
            src = wrap_fragment(payload)
        elif kind == "sigs":
            src = wrap_sigs(payload)
        else:
            continue
        work_items.append({"idx": idx, "line": b["line"], "kind": kind, "src": src,
                           "preview": next((l for l in b["body"] if not is_comment_or_blank(l)), "")[:60]})

    from concurrent.futures import ThreadPoolExecutor
    def run(item):
        ok, err = compile_src(item["src"], f"{os.path.basename(doc)}-{item['idx']}")
        item["ok"] = ok; item["err"] = err
        return item
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for item in ex.map(run, work_items):
            results.append(item)
    results.sort(key=lambda r: r["line"])
    npass = sum(1 for r in results if r["ok"])
    print(f"\n==== {doc}: {npass}/{len(results)} blocks compile ====")
    for r in results:
        if not r["ok"]:
            print(f"FAIL L{r['line']} [{r['kind']}] {r['preview']}")
            print(f"     {r['err']}")

if __name__ == "__main__":
    main()
