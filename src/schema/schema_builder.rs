use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};

use super::symbol_table::SymbolTable;
use super::type_mapping::{parse_var_type, FieldType};
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct EntitySchema {
    pub serializer_name: Arc<str>,
    pub serializer_hash: u64,
    pub arrow_schema: Arc<Schema>,
    pub field_keys: Vec<u64>,
    pub field_column_counts: Vec<usize>,
}

impl EntitySchema {
    pub fn project(&self, projection: &[usize]) -> Self {
        let all_fields = self.arrow_schema.fields();

        let mut projected_arrow_fields = Vec::new();
        let mut projected_field_keys = Vec::new();
        let mut projected_field_column_counts = Vec::new();

        let mut arrow_col_idx = 0;
        for i in 0..3 {
            if projection.contains(&i) {
                projected_arrow_fields.push(all_fields[i].clone());
            }
            arrow_col_idx += 1;
        }

        for (field_idx, &key) in self.field_keys.iter().enumerate() {
            let column_count = self.field_column_counts[field_idx];
            let field_start_col = arrow_col_idx;

            let field_projected =
                (0..column_count).any(|offset| projection.contains(&(field_start_col + offset)));

            if field_projected {
                for offset in 0..column_count {
                    projected_arrow_fields.push(all_fields[field_start_col + offset].clone());
                }
                projected_field_keys.push(key);
                projected_field_column_counts.push(column_count);
            }

            arrow_col_idx += column_count;
        }

        EntitySchema {
            serializer_name: Arc::clone(&self.serializer_name),
            serializer_hash: self.serializer_hash,
            arrow_schema: Arc::new(Schema::new(projected_arrow_fields)),
            field_keys: projected_field_keys,
            field_column_counts: projected_field_column_counts,
        }
    }
}

pub struct SchemaBuilder<'a> {
    _symbol_table: &'a SymbolTable,
}

impl<'a> SchemaBuilder<'a> {
    pub fn new(symbol_table: &'a SymbolTable) -> Self {
        Self {
            _symbol_table: symbol_table,
        }
    }

    pub fn build_schema(
        &self,
        serializer_name: &str,
        serializer_hash: u64,
        fields: &[FieldInfo],
    ) -> Result<EntitySchema> {
        let mut arrow_fields = Vec::new();
        let mut field_keys = Vec::new();
        let mut field_column_counts = Vec::new();
        let mut seen_names: HashSet<String> = HashSet::new();

        arrow_fields.push(Field::new("tick", DataType::Int32, false));
        arrow_fields.push(Field::new("entity_index", DataType::Int32, false));
        arrow_fields.push(Field::new("delta_type", DataType::Utf8, false));
        seen_names.insert("tick".to_string());
        seen_names.insert("entity_index".to_string());
        seen_names.insert("delta_type".to_string());

        for field_info in fields {
            let field_type = parse_var_type(&field_info.var_type);
            let base_name = super::symbol_table::build_field_name(
                field_info.send_node.as_deref(),
                &field_info.var_name,
            );

            match &field_type {
                FieldType::Vector2 { base } => {
                    let mut all_unique = true;
                    for suffix in field_type.component_suffixes() {
                        let name = format!("{}{}", base_name, suffix);
                        if seen_names.contains(&name) {
                            all_unique = false;
                            break;
                        }
                    }
                    if all_unique {
                        for suffix in field_type.component_suffixes() {
                            let name = format!("{}{}", base_name, suffix);
                            seen_names.insert(name.clone());
                            arrow_fields.push(Field::new(&name, base.clone(), true));
                        }
                        field_keys.push(field_info.key);
                        field_column_counts.push(2);
                    }
                }
                FieldType::Vector3 { base } => {
                    let mut all_unique = true;
                    for suffix in field_type.component_suffixes() {
                        let name = format!("{}{}", base_name, suffix);
                        if seen_names.contains(&name) {
                            all_unique = false;
                            break;
                        }
                    }
                    if all_unique {
                        for suffix in field_type.component_suffixes() {
                            let name = format!("{}{}", base_name, suffix);
                            seen_names.insert(name.clone());
                            arrow_fields.push(Field::new(&name, base.clone(), true));
                        }
                        field_keys.push(field_info.key);
                        field_column_counts.push(3);
                    }
                }
                FieldType::Vector4 { base } => {
                    let mut all_unique = true;
                    for suffix in field_type.component_suffixes() {
                        let name = format!("{}{}", base_name, suffix);
                        if seen_names.contains(&name) {
                            all_unique = false;
                            break;
                        }
                    }
                    if all_unique {
                        for suffix in field_type.component_suffixes() {
                            let name = format!("{}{}", base_name, suffix);
                            seen_names.insert(name.clone());
                            arrow_fields.push(Field::new(&name, base.clone(), true));
                        }
                        field_keys.push(field_info.key);
                        field_column_counts.push(4);
                    }
                }
                FieldType::Nested { .. } => {
                    continue;
                }
                _ => {
                    if seen_names.contains(&base_name) {
                        continue;
                    }
                    seen_names.insert(base_name.clone());
                    let arrow_type = field_type.to_arrow_type();
                    arrow_fields.push(Field::new(&base_name, arrow_type, true));
                    field_keys.push(field_info.key);
                    field_column_counts.push(1);
                }
            }
        }

        Ok(EntitySchema {
            serializer_name: Arc::from(serializer_name),
            serializer_hash,
            arrow_schema: Arc::new(Schema::new(arrow_fields)),
            field_keys,
            field_column_counts,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FieldInfo {
    pub var_name: String,
    pub var_type: String,
    pub send_node: Option<String>,
    pub key: u64,
}

impl FieldInfo {
    pub fn new(var_name: String, var_type: String, send_node: Option<String>, key: u64) -> Self {
        Self {
            var_name,
            var_type,
            send_node,
            key,
        }
    }
}

pub fn metadata_columns() -> Vec<Field> {
    vec![
        Field::new("tick", DataType::Int32, false),
        Field::new("entity_index", DataType::Int32, false),
        Field::new("delta_type", DataType::Utf8, false),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_simple_schema() {
        let symbols = vec![
            "m_iHealth".to_string(),
            "m_vecOrigin".to_string(),
            "int32".to_string(),
            "Vector".to_string(),
        ];
        let symbol_table = SymbolTable::new(symbols);
        let builder = SchemaBuilder::new(&symbol_table);

        let fields = vec![
            FieldInfo::new("m_iHealth".to_string(), "int32".to_string(), None, 1),
            FieldInfo::new("m_vecOrigin".to_string(), "Vector".to_string(), None, 2),
        ];

        let schema = builder.build_schema("TestEntity", 12345, &fields).unwrap();

        assert_eq!(&*schema.serializer_name, "TestEntity");
        assert_eq!(schema.arrow_schema.fields().len(), 3 + 1 + 3);

        let field_names: Vec<_> = schema
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        assert!(field_names.contains(&"tick"));
        assert!(field_names.contains(&"entity_index"));
        assert!(field_names.contains(&"delta_type"));
        assert!(field_names.contains(&"m_iHealth"));
        assert!(field_names.contains(&"m_vecOrigin__x"));
        assert!(field_names.contains(&"m_vecOrigin__y"));
        assert!(field_names.contains(&"m_vecOrigin__z"));
    }

    #[test]
    fn test_schema_with_send_node() {
        let symbols = vec!["m_nHeroID".to_string(), "int32".to_string()];
        let symbol_table = SymbolTable::new(symbols);
        let builder = SchemaBuilder::new(&symbol_table);

        let fields = vec![FieldInfo::new(
            "m_nHeroID".to_string(),
            "int32".to_string(),
            Some("m_CCitadelHeroComponent.m_loadingHero".to_string()),
            1,
        )];

        let schema = builder
            .build_schema("CCitadelPlayerPawn", 12345, &fields)
            .unwrap();

        let field_names: Vec<_> = schema
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        assert!(field_names.contains(&"m_CCitadelHeroComponent__m_loadingHero__m_nHeroID"));
    }

    #[test]
    fn test_project_schema() {
        let symbols = vec![
            "m_iHealth".to_string(),
            "m_iArmor".to_string(),
            "m_vecOrigin".to_string(),
            "int32".to_string(),
            "Vector".to_string(),
        ];
        let symbol_table = SymbolTable::new(symbols);
        let builder = SchemaBuilder::new(&symbol_table);

        let fields = vec![
            FieldInfo::new("m_iHealth".to_string(), "int32".to_string(), None, 1),
            FieldInfo::new("m_iArmor".to_string(), "int32".to_string(), None, 2),
            FieldInfo::new("m_vecOrigin".to_string(), "Vector".to_string(), None, 3),
        ];

        let schema = builder.build_schema("TestEntity", 12345, &fields).unwrap();
        assert_eq!(schema.arrow_schema.fields().len(), 8);
        assert_eq!(schema.field_keys.len(), 3);

        let projected = schema.project(&[0, 1, 2, 3]);

        assert_eq!(projected.arrow_schema.fields().len(), 4);
        assert_eq!(projected.field_keys.len(), 1);
        assert_eq!(projected.field_keys[0], 1);

        let field_names: Vec<_> = projected
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"tick"));
        assert!(field_names.contains(&"entity_index"));
        assert!(field_names.contains(&"delta_type"));
        assert!(field_names.contains(&"m_iHealth"));
        assert!(!field_names.contains(&"m_iArmor"));
        assert!(!field_names.contains(&"m_vecOrigin__x"));
    }

    #[test]
    fn test_project_schema_vector_field() {
        let symbols = vec![
            "m_iHealth".to_string(),
            "m_vecOrigin".to_string(),
            "int32".to_string(),
            "Vector".to_string(),
        ];
        let symbol_table = SymbolTable::new(symbols);
        let builder = SchemaBuilder::new(&symbol_table);

        let fields = vec![
            FieldInfo::new("m_iHealth".to_string(), "int32".to_string(), None, 1),
            FieldInfo::new("m_vecOrigin".to_string(), "Vector".to_string(), None, 2),
        ];

        let schema = builder.build_schema("TestEntity", 12345, &fields).unwrap();

        let projected = schema.project(&[0, 1, 2, 4]);

        assert_eq!(projected.field_keys.len(), 1);
        assert_eq!(projected.field_keys[0], 2);
        assert_eq!(projected.field_column_counts[0], 3);

        let field_names: Vec<_> = projected
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert!(field_names.contains(&"m_vecOrigin__x"));
        assert!(field_names.contains(&"m_vecOrigin__y"));
        assert!(field_names.contains(&"m_vecOrigin__z"));
        assert!(!field_names.contains(&"m_iHealth"));
    }
}
