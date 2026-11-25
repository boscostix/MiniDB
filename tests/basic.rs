use minidb::{Error, MiniDB, Options, Result};

#[test]
fn inserts_retrieves_and_replaces_values() -> Result<()> {
    let db = MiniDB::open("data")?;
    assert_eq!(db.get(b"name")?, None);
    db.put(b"name", b"Alice")?;
    assert_eq!(db.get(b"name")?, Some(b"Alice".to_vec()));
    db.put(b"name", b"Bob")?;
    assert_eq!(db.get(b"name")?, Some(b"Bob".to_vec()));
    Ok(())
}

#[test]
fn deletes_present_and_missing_keys_and_allows_reinsertion() -> Result<()> {
    let db = MiniDB::open("data")?;
    db.put(b"present", b"original")?;
    db.delete(b"present")?;
    db.delete(b"missing")?;
    assert_eq!(db.get(b"present")?, None);
    assert_eq!(db.get(b"missing")?, None);
    db.delete(b"present")?;
    db.put(b"present", b"replacement")?;
    assert_eq!(db.get(b"present")?, Some(b"replacement".to_vec()));
    Ok(())
}

#[test]
fn handles_binary_keys_and_values_and_returns_owned_bytes() -> Result<()> {
    let db = MiniDB::open("data")?;
    let mut key = vec![0, 255, 128];
    let mut value = vec![255, 0, 128];
    db.put(&key, &value)?;
    key.fill(0);
    value.fill(0);
    let original_key = b"\0\xff\x80";
    let mut returned = db.get(original_key)?.expect("binary key is present");
    assert_eq!(returned, b"\xff\0\x80");
    returned.fill(0);
    assert_eq!(db.get(original_key)?, Some(b"\xff\0\x80".to_vec()));
    assert_eq!(db.get(&key)?, None);
    Ok(())
}

#[test]
fn distinguishes_empty_values_from_absence() -> Result<()> {
    let db = MiniDB::open("data")?;
    db.put(b"key", b"")?;
    assert_eq!(db.get(b"key")?, Some(Vec::new()));
    db.delete(b"key")?;
    assert_eq!(db.get(b"key")?, None);
    Ok(())
}

#[test]
fn rejects_empty_keys_in_all_key_operations() -> Result<()> {
    let db = MiniDB::open("data")?;
    assert_eq!(db.put(b"", b"value"), Err(Error::EmptyKey));
    assert_eq!(db.get(b""), Err(Error::EmptyKey));
    assert_eq!(db.delete(b""), Err(Error::EmptyKey));
    Ok(())
}

#[test]
fn enforces_configured_limits_without_changing_existing_values() -> Result<()> {
    let db = MiniDB::open_with_options(
        "data",
        Options {
            max_key_bytes: Some(3),
            max_value_bytes: Some(2),
        },
    )?;
    db.put(b"key", b"ok")?;
    let oversized_key = Error::KeyTooLarge { len: 4, max: 3 };
    assert_eq!(db.put(b"keys", b"ok"), Err(oversized_key.clone()));
    assert_eq!(db.get(b"keys"), Err(oversized_key.clone()));
    assert_eq!(db.delete(b"keys"), Err(oversized_key));
    assert_eq!(
        db.put(b"key", b"bad"),
        Err(Error::ValueTooLarge { len: 3, max: 2 })
    );
    assert_eq!(db.get(b"key")?, Some(b"ok".to_vec()));
    Ok(())
}

#[test]
fn allows_empty_values_with_a_zero_byte_value_limit() -> Result<()> {
    let db = MiniDB::open_with_options(
        "data",
        Options {
            max_value_bytes: Some(0),
            ..Options::default()
        },
    )?;
    db.put(b"key", b"")?;
    assert_eq!(
        db.put(b"key", b"a"),
        Err(Error::ValueTooLarge { len: 1, max: 0 })
    );
    assert_eq!(db.get(b"key")?, Some(Vec::new()));
    Ok(())
}

#[test]
fn handles_repeated_operations_without_affecting_other_keys() -> Result<()> {
    let db = MiniDB::open("data")?;
    db.put(b"stable", b"value")?;
    for byte in 0..=255 {
        db.put(b"key", &[byte])?;
        assert_eq!(db.get(b"key")?, Some(vec![byte]));
        db.delete(b"key")?;
        assert_eq!(db.get(b"key")?, None);
    }
    assert_eq!(db.get(b"stable")?, Some(b"value".to_vec()));
    Ok(())
}

#[test]
fn creates_independent_instances_and_releases_data_on_close() -> Result<()> {
    let db = MiniDB::open("data")?;
    let other = MiniDB::open("data")?;
    db.put(b"key", b"value")?;
    assert_eq!(other.get(b"key")?, None);
    assert_eq!(db.flush(), Err(Error::PersistenceUnavailable));
    assert_eq!(db.get(b"key")?, Some(b"value".to_vec()));
    db.close()?;
    assert_eq!(MiniDB::open("data")?.get(b"key")?, None);
    Ok(())
}
