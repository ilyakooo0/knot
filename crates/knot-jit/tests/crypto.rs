//! Crypto builtins: X25519 sealed-box encrypt/decrypt, Ed25519 sign/verify,
//! and key generation. All return Maybe/Bool on bad input (never abort).
//!
//! Subprocess — these need IO {random} and real process execution.

mod e2e;
use e2e::assert_stdout;

#[test]
fn encrypt_decrypt_roundtrip() {
    assert_stdout(
        "crypto_rt",
        r#"(do
  keys <- base.generateKeyPair
  with {  msg (base.textToBytes "secret") } (do
    enc <- base.encrypt keys.publicKey msg
    match enc
      Maybe.Nothing {}  base.println "ENCRYPT-FAILED"
      Maybe.Just {value ct}  match base.decrypt keys.privateKey ct
        Maybe.Nothing {}  base.println "DECRYPT-FAILED"
        Maybe.Just {value pt}  match base.bytesToText pt
          Maybe.Just {value t}  base.println t
          Maybe.Nothing {}  base.println "BAD-UTF8"
    yield {}))"#,
        "\"secret\"\n{}",
    );
}

#[test]
fn decrypt_rejects_tampered_ciphertext() {
    assert_stdout(
        "crypto_tamper",
        r#"(do
  keys <- base.generateKeyPair
  enc <- base.encrypt keys.publicKey (base.textToBytes "data")
  match enc
    Maybe.Nothing {}  base.println "ENCRYPT-FAILED"
    Maybe.Just {value ct}
      with {  tampered (base.bytesConcat (base.bytesSlice 0 40 ct) (base.textToBytes "XXXX")) } (do
        match base.decrypt keys.privateKey tampered
          Maybe.Nothing {}  base.println "rejected"
          Maybe.Just {value _}  base.println "BUG-DECRYPTED"
        yield {})
  yield {})"#,
        "\"rejected\"\n{}",
    );
}

#[test]
fn decrypt_with_wrong_key_fails() {
    assert_stdout(
        "crypto_wrongkey",
        r#"(do
  alice <- base.generateKeyPair
  bob <- base.generateKeyPair
  enc <- base.encrypt alice.publicKey (base.textToBytes "for alice")
  match enc
    Maybe.Nothing {}  base.println "ENCRYPT-FAILED"
    Maybe.Just {value ct}  match base.decrypt bob.privateKey ct
      Maybe.Nothing {}  base.println "rejected"
      Maybe.Just {value _}  base.println "BUG-WRONGKEY"
  yield {})"#,
        "\"rejected\"\n{}",
    );
}

#[test]
fn sign_verify_roundtrip() {
    assert_stdout(
        "crypto_sign",
        r#"(do
  keys <- base.generateSigningKeyPair
  with {  msg (base.textToBytes "message") } (do
    match base.sign keys.privateKey msg
      Maybe.Nothing {}  base.println "SIGN-FAILED"
      Maybe.Just {value sig}
        with {  ok (base.verify keys.publicKey msg sig) } (do
          base.println (base.show ok)
          yield {})
    yield {}))"#,
        "\"True\"\n{}",
    );
}

#[test]
fn verify_rejects_wrong_message() {
    assert_stdout(
        "crypto_verifybad",
        r#"(do
  keys <- base.generateSigningKeyPair
  match base.sign keys.privateKey (base.textToBytes "original")
    Maybe.Nothing {}  base.println "SIGN-FAILED"
    Maybe.Just {value sig}
      with {  bad (base.verify keys.publicKey (base.textToBytes "forged") sig) } (do
        base.println (base.show bad)
        yield {})
  yield {})"#,
        "\"False\"\n{}",
    );
}

#[test]
fn key_lengths_are_32_bytes() {
    assert_stdout(
        "crypto_keylen",
        r#"(do
  enc <- base.generateKeyPair
  sig <- base.generateSigningKeyPair
  base.println (base.show (base.bytesLength enc.publicKey))
  base.println (base.show (base.bytesLength enc.privateKey))
  base.println (base.show (base.bytesLength sig.publicKey))
  base.println (base.show (base.bytesLength sig.privateKey))
  yield {})"#,
        "\"32\"\n\"32\"\n\"32\"\n\"32\"\n{}",
    );
}