//! Extract human-readable field names from the CSVCMsg_FlattenedSerializer protobuf.
//!
//! haste's `Symbol` only stores a hash (u64), not the original string.
//! This module parses the `repeated string symbols` field from the protobuf
//! to build a hash → name lookup table.
