#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// Flag 1 in the RELATION message: the column is part of the replica identity.
    pub is_key: bool,
    pub type_oid: u32,
    /// Signed: -1 means "no modifier set".
    pub atttypmod: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    /// relreplident from pg_class: b'd' DEFAULT, b'n' NOTHING, b'f' FULL, b'i' INDEX.
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

use std::collections::HashMap;

/// A cache of table descriptions, scoped to a single replication session.
/// Row messages reference a table by OID and carry no column names —
/// names are looked up here by index.
#[derive(Debug, Default)]
pub struct RelationCache {
    by_id: HashMap<u32, Relation>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// A repeated RELATION for a known OID replaces the entry entirely.
    pub fn put(&mut self, relation: Relation) {
        self.by_id.insert(relation.id, relation);
    }

    pub fn get(&self, id: u32) -> Option<&Relation> {
        self.by_id.get(&id)
    }

    /// A full reset. Called on reconnect: the server will resend RELATION
    /// before the first row message for each table in the new session.
    pub fn clear(&mut self) {
        self.by_id.clear();
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(id: u32, name: &str, cols: &[&str]) -> Relation {
        Relation {
            id,
            namespace: "public".into(),
            name: name.into(),
            replica_identity: b'd',
            columns: cols
                .iter()
                .map(|c| Column {
                    name: (*c).into(),
                    is_key: false,
                    type_oid: 25,
                    atttypmod: -1,
                })
                .collect(),
        }
    }

    #[test]
    fn repeated_relation_for_same_oid_replaces_the_entry() {
        // A repeated RELATION is a routine message (DDL, a replica identity change,
        // a publication change), and it must REPLACE the entry, rather than being an error
        // or being ignored.
        let mut cache = RelationCache::new();
        cache.put(rel(1, "users", &["id", "name"]));
        cache.put(rel(1, "users", &["id", "name", "email"]));
        assert_eq!(
            cache.len(),
            1,
            "the same OID does not create a second entry"
        );
        assert_eq!(cache.get(1).unwrap().columns.len(), 3, "the new schema won");
    }

    #[test]
    fn clear_drops_everything() {
        // The cache is scoped to the replication session: on reconnect it is reset entirely,
        // because the server will resend RELATION, and the old schema may be stale.
        let mut cache = RelationCache::new();
        cache.put(rel(1, "users", &["id"]));
        cache.put(rel(2, "items", &["id"]));
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.get(1).is_none());
    }
}
