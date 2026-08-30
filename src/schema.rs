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
