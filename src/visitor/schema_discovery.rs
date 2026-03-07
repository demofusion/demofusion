use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::haste::demostream::CmdHeader;
use crate::haste::flattenedserializers::{FlattenedSerializer, FlattenedSerializerField};
use crate::haste::parser::{Context, Visitor};
use crate::haste::valveprotos::common::EDemoCommands;
use prost::Message;

use crate::error::{Result, Source2DfError};
use crate::schema::{compute_field_key, EntitySchema, FieldInfo, SchemaBuilder, SymbolTable};

pub struct SchemaDiscoveryVisitor {
    symbols: Arc<Mutex<Option<Vec<String>>>>,
    serializers_discovered: bool,
    sync_tick_reached: bool,
}

impl SchemaDiscoveryVisitor {
    pub fn new() -> Self {
        Self {
            symbols: Arc::new(Mutex::new(None)),
            serializers_discovered: false,
            sync_tick_reached: false,
        }
    }

    pub fn with_shared_symbols(symbols: Arc<Mutex<Option<Vec<String>>>>) -> Self {
        Self {
            symbols,
            serializers_discovered: false,
            sync_tick_reached: false,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.sync_tick_reached && self.serializers_discovered
    }

    pub fn symbols_handle(&self) -> Arc<Mutex<Option<Vec<String>>>> {
        self.symbols.clone()
    }
}

impl Default for SchemaDiscoveryVisitor {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum SchemaDiscoveryError {
    Discovery(String),
    DiscoveryComplete,
}

impl SchemaDiscoveryError {
    pub fn is_complete(&self) -> bool {
        matches!(self, SchemaDiscoveryError::DiscoveryComplete)
    }
}

impl std::fmt::Display for SchemaDiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaDiscoveryError::Discovery(msg) => write!(f, "Schema discovery error: {}", msg),
            SchemaDiscoveryError::DiscoveryComplete => write!(f, "Schema discovery complete"),
        }
    }
}

impl std::error::Error for SchemaDiscoveryError {}

impl Visitor for SchemaDiscoveryVisitor {
    type Error = SchemaDiscoveryError;

    async fn on_cmd(
        &mut self,
        _ctx: &Context,
        cmd_header: &CmdHeader,
        data: &[u8],
    ) -> std::result::Result<(), Self::Error> {
        match cmd_header.cmd {
            EDemoCommands::DemSendTables => {
                if let Ok(symbols) = extract_symbols_from_send_tables(data) {
                    if let Ok(mut guard) = self.symbols.lock() {
                        *guard = Some(symbols);
                    }
                }
                self.serializers_discovered = true;
            }
            EDemoCommands::DemSyncTick => {
                self.sync_tick_reached = true;
                return Err(SchemaDiscoveryError::DiscoveryComplete);
            }
            _ => {}
        }
        Ok(())
    }
}

fn extract_symbols_from_send_tables(data: &[u8]) -> Result<Vec<String>> {
    use crate::haste::valveprotos::common::CsvcMsgFlattenedSerializer;

    let send_tables = crate::haste::valveprotos::common::CDemoSendTables::decode(data)
        .map_err(|e| Source2DfError::Haste(format!("Failed to decode send tables: {}", e)))?;

    let raw_data = send_tables.data.unwrap_or_default();
    let mut data_slice = &raw_data[..];

    let (_size, _count) = read_uvarint64(&mut data_slice)
        .map_err(|e| Source2DfError::Haste(format!("Failed to read varint: {}", e)))?;

    let msg = CsvcMsgFlattenedSerializer::decode(data_slice)
        .map_err(|e| Source2DfError::Haste(format!("Failed to decode flattened serializer: {}", e)))?;

    Ok(msg.symbols.into_iter().collect())
}

fn read_uvarint64(data: &mut &[u8]) -> std::result::Result<(u64, u64), &'static str> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    let mut count = 0u64;

    loop {
        if data.is_empty() {
            return Err("unexpected end of data");
        }
        let byte = data[0];
        *data = &data[1..];
        count += 1;

        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint too long");
        }
    }

    Ok((result, count))
}

pub struct DiscoveredSchemas {
    pub symbol_table: SymbolTable,
    pub schemas: HashMap<u64, EntitySchema>,
}

impl DiscoveredSchemas {
    pub fn from_context(ctx: &Context, symbols: Vec<String>) -> Result<Self> {
        let symbol_table = SymbolTable::new(symbols);
        let builder = SchemaBuilder::new(&symbol_table);

        let serializers = ctx
            .serializers()
            .ok_or_else(|| Source2DfError::Schema("No serializers in context".to_string()))?;

        let mut schemas = HashMap::new();

        for serializer in serializers.values() {
            let serializer_name = symbol_table
                .resolve(serializer.serializer_name.hash)
                .unwrap_or("unknown")
                .to_string();

            // Collect fields, recursing into nested serializers (e.g., CBodyComponent)
            // to extract their child fields with flattened names.
            //
            // OPTION B (Arrow Struct) FUTURE IMPLEMENTATION:
            // Instead of flattening nested fields into separate columns, we could:
            // 1. Create Arrow Struct types for nested serializers
            // 2. Store FieldInfo with a `children: Vec<FieldInfo>` for nested fields
            // 3. In SchemaBuilder, create DataType::Struct with nested Field definitions
            // 4. In BatchAccumulator, use StructBuilder to build nested values
            // 5. SQL access would use dot notation: "CBodyComponent"."m_cellX"
            //
            // Benefits of Struct approach:
            // - Cleaner schema with logical grouping
            // - Better compatibility with Parquet nested types
            // - More natural SQL access patterns
            //
            // Current approach (Option A): Flatten to columns like "CBodyComponent__m_cellX"
            // This is simpler and matches how the parser stores field values with composite keys.
            let mut fields = Vec::new();
            collect_fields_recursive(
                serializer,
                &symbol_table,
                None,  // parent_path
                None,  // parent_key
                &mut fields,
                0,     // depth
            );

            if let Ok(schema) = builder.build_schema(
                &serializer_name,
                serializer.serializer_name.hash,
                &fields,
            ) {
                schemas.insert(serializer.serializer_name.hash, schema);
            }
        }

        Ok(Self {
            symbol_table,
            schemas,
        })
    }

    pub fn get_schema(&self, serializer_hash: u64) -> Option<&EntitySchema> {
        self.schemas.get(&serializer_hash)
    }

    pub fn serializer_names(&self) -> Vec<&str> {
        self.schemas
            .values()
            .map(|s| &*s.serializer_name)
            .collect()
    }
}

/// Maximum recursion depth for nested serializers to prevent infinite loops.
/// In practice, Source 2 demos rarely have more than 2-3 levels of nesting.
const MAX_NESTED_DEPTH: usize = 5;

/// Recursively collects fields from a serializer, including nested fields from
/// embedded components like CBodyComponent.
///
/// # Arguments
/// * `serializer` - The serializer to extract fields from
/// * `symbol_table` - Symbol table for resolving hashes to names
/// * `parent_path` - The path prefix for nested fields (e.g., "CBodyComponent")
/// * `parent_key` - The hash key of the parent field (for computing composite keys)
/// * `fields` - Output vector to collect FieldInfo entries
/// * `depth` - Current recursion depth (for safety limit)
///
/// # Option B (Arrow Struct) notes:
/// To implement Arrow Struct types instead of flattening:
/// 1. Change FieldInfo to have an optional `children: Vec<FieldInfo>` field
/// 2. Instead of flattening, create a single FieldInfo with nested children
/// 3. Return the children in a structured way that SchemaBuilder can convert to DataType::Struct
fn collect_fields_recursive(
    serializer: &FlattenedSerializer,
    symbol_table: &SymbolTable,
    parent_path: Option<&str>,
    parent_key: Option<u64>,
    fields: &mut Vec<FieldInfo>,
    depth: usize,
) {
    if depth > MAX_NESTED_DEPTH {
        return;
    }

    for field in &serializer.fields {
        // Check if this field has a nested serializer (e.g., CBodyComponent types)
        // These are marked with FieldSpecialDescriptor::Pointer in fieldmetadata.rs
        //
        // For Pointer types, we DON'T add the parent field itself - we only recurse
        // into the nested serializer to extract its leaf fields.
        // For other types with field_serializer (arrays), we DO add the field.
        //
        // Option B note: Instead of recursing and flattening, we would:
        // 1. Create a FieldInfo with field_type = FieldType::Struct { children }
        // 2. Recursively build the children Vec<FieldInfo>
        // 3. Let SchemaBuilder create a DataType::Struct from the children
        let is_pointer = field.is_pointer();
        
        if let Some(nested_serializer) = &field.field_serializer {
            if is_pointer {
                // For Pointer types (CBodyComponent, etc.), recurse into nested fields
                // but don't add the parent field itself (it's just a bool indicating presence)
                let var_name = match symbol_table.resolve(field.var_name.hash) {
                    Some(name) => name,
                    None => continue,
                };
                
                let nested_path = match parent_path {
                    Some(pp) => format!("{}.{}", pp, var_name),
                    None => var_name.to_string(),
                };
                
                // Compute the composite key for the parent field
                // This must match how the parser builds field_key in entities.rs
                let nested_key = match parent_key {
                    Some(pk) => crate::haste::fxhash::add_u64_to_hash(pk, field.var_name.hash),
                    None => field.var_name.hash,
                };
                
                collect_fields_recursive(
                    nested_serializer,
                    symbol_table,
                    Some(&nested_path),
                    Some(nested_key),
                    fields,
                    depth + 1,
                );
                continue;  // Don't add the pointer field itself
            }
        }
        
        // Add leaf fields (non-pointer fields)
        if let Some(field_info) = extract_field_info(field, symbol_table, parent_path, parent_key) {
            fields.push(field_info);
        }
    }
}

/// Extracts a FieldInfo from a single field, applying parent path prefix if present.
///
/// # Option B (Arrow Struct) notes:
/// For struct types, this would return a FieldInfo with:
/// - field_type indicating it's a struct
/// - children populated by recursive calls
fn extract_field_info(
    field: &FlattenedSerializerField,
    symbol_table: &SymbolTable,
    parent_path: Option<&str>,
    parent_key: Option<u64>,
) -> Option<FieldInfo> {
    let var_name = symbol_table.resolve(field.var_name.hash)?;
    let var_type = symbol_table.resolve(field.var_type.hash)?;
    
    // Build the send_node path by combining parent_path with the field's own send_node
    let field_send_node = field
        .send_node
        .as_ref()
        .and_then(|sn| symbol_table.resolve(sn.hash));
    
    let send_node = match (parent_path, field_send_node) {
        (Some(pp), Some(sn)) => Some(format!("{}.{}", pp, sn)),
        (Some(pp), None) => Some(pp.to_string()),
        (None, Some(sn)) => Some(sn.to_string()),
        (None, None) => None,
    };
    
    // Compute the field key
    // For nested fields, we need to combine the parent key with this field's var_name hash
    // This must match how the parser computes field_key in entities.rs:
    //   field_key = fxhash::add_u64_to_hash(field_key, field.var_name.hash);
    let key = match parent_key {
        Some(pk) => crate::haste::fxhash::add_u64_to_hash(pk, field.var_name.hash),
        None => compute_field_key(send_node.as_deref(), var_name),
    };
    
    Some(FieldInfo::new(
        var_name.to_string(),
        var_type.to_string(),
        send_node,
        key,
    ))
}

/// Discover entity schemas from a GOTV broadcast /start packet.
///
/// Parses the broadcast packet to extract entity serializer definitions and builds
/// Arrow schemas for each entity type. This should be called with the response body
/// from the GOTV broadcast `/start` endpoint.
pub async fn discover_schemas_from_broadcast(data: &[u8]) -> Result<Vec<EntitySchema>> {
    use crate::haste::core::async_broadcast_stream::AsyncBroadcastStream;
    use crate::haste::parser::AsyncStreamingParser;

    let broadcast_stream = AsyncBroadcastStream::from_bytes(data.to_vec());

    let visitor = SchemaDiscoveryVisitor::new();
    let symbols_handle = visitor.symbols_handle();

    let mut parser = AsyncStreamingParser::from_stream_with_visitor(broadcast_stream, visitor)
        .map_err(|e| Source2DfError::Haste(e.to_string()))?;

    // Run until schema discovery completes (at DEM_SyncTick)
    let _ = parser.run_to_end().await;

    let ctx = parser.context();
    let symbols = symbols_handle
        .lock()
        .map_err(|_| Source2DfError::Schema("Failed to lock symbols".to_string()))?
        .take()
        .ok_or_else(|| Source2DfError::Schema("No symbols discovered".to_string()))?;

    let discovered = DiscoveredSchemas::from_context(ctx, symbols)?;
    Ok(discovered.schemas.into_values().collect())
}
