#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// Флаг 1 в сообщении RELATION: колонка входит в replica identity.
    pub is_key: bool,
    pub type_oid: u32,
    /// Знаковый: -1 означает «модификатор не задан».
    pub atttypmod: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    /// relreplident из pg_class: b'd' DEFAULT, b'n' NOTHING, b'f' FULL, b'i' INDEX.
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

use std::collections::HashMap;

/// Кэш описаний таблиц, живущий в рамках одной сессии репликации.
/// Row-сообщения ссылаются на таблицу по OID и не несут имён колонок —
/// имена берутся отсюда по индексу.
#[derive(Debug, Default)]
pub struct RelationCache {
    by_id: HashMap<u32, Relation>,
}

impl RelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Повторный RELATION для известного OID заменяет запись целиком.
    pub fn put(&mut self, relation: Relation) {
        self.by_id.insert(relation.id, relation);
    }

    pub fn get(&self, id: u32) -> Option<&Relation> {
        self.by_id.get(&id)
    }

    /// Полный сброс. Вызывается при реконнекте: сервер перешлёт RELATION
    /// перед первым row-сообщением каждой таблицы в новой сессии.
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
        // Повторный RELATION — штатное сообщение (DDL, смена replica identity,
        // изменение публикации), и он обязан ЗАМЕНИТЬ запись, а не быть ошибкой
        // и не быть проигнорированным.
        let mut cache = RelationCache::new();
        cache.put(rel(1, "users", &["id", "name"]));
        cache.put(rel(1, "users", &["id", "name", "email"]));
        assert_eq!(cache.len(), 1, "тот же OID не создаёт вторую запись");
        assert_eq!(
            cache.get(1).unwrap().columns.len(),
            3,
            "победила новая схема"
        );
    }

    #[test]
    fn clear_drops_everything() {
        // Кэш живёт в рамках сессии репликации: при реконнекте сбрасывается целиком,
        // потому что сервер перешлёт RELATION заново, а старая схема может быть устаревшей.
        let mut cache = RelationCache::new();
        cache.put(rel(1, "users", &["id"]));
        cache.put(rel(2, "items", &["id"]));
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.get(1).is_none());
    }
}
