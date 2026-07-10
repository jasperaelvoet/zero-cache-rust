//! In-memory implementation of the v1.7 operator `Storage` contract.

use std::cell::RefCell;
use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

use crate::ivm::operator::{Storage, StorageError};

#[derive(Debug, Default)]
pub struct MemoryStorage {
    data: RefCell<BTreeMap<String, JsonValue>>,
}

impl MemoryStorage {
    pub fn clone_data(&self) -> BTreeMap<String, JsonValue> {
        self.data.borrow().clone()
    }
}

impl Storage for MemoryStorage {
    fn set(&self, key: &str, value: JsonValue) -> Result<(), StorageError> {
        self.data.borrow_mut().insert(key.to_string(), value);
        Ok(())
    }

    fn get(
        &self,
        key: &str,
        default: Option<JsonValue>,
    ) -> Result<Option<JsonValue>, StorageError> {
        Ok(self.data.borrow().get(key).cloned().or(default))
    }

    fn scan(&self, prefix: Option<&str>) -> Result<Vec<(String, JsonValue)>, StorageError> {
        let prefix = prefix.unwrap_or_default();
        Ok(self
            .data
            .borrow()
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn del(&self, key: &str) -> Result<(), StorageError> {
        self.data.borrow_mut().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_round_trip_and_prefix_scan_match_upstream() {
        let storage = MemoryStorage::default();
        storage.set("a/2", JsonValue::Number(2.0)).unwrap();
        storage.set("b/1", JsonValue::Number(3.0)).unwrap();
        storage.set("a/1", JsonValue::Number(1.0)).unwrap();

        assert_eq!(
            storage.scan(Some("a/")).unwrap(),
            vec![
                ("a/1".into(), JsonValue::Number(1.0)),
                ("a/2".into(), JsonValue::Number(2.0)),
            ]
        );
        assert_eq!(
            storage.get("missing", Some(JsonValue::Bool(true))).unwrap(),
            Some(JsonValue::Bool(true))
        );
        storage.del("a/1").unwrap();
        assert_eq!(storage.get("a/1", None).unwrap(), None);
    }
}
