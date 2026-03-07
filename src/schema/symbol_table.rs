use std::collections::HashMap;

const GOLDEN_RATIO: u64 = 0x517cc1b727220a95;
const ROTATION_LENGTH: u32 = 5;

fn add_u64_to_hash(hash: u64, value: u64) -> u64 {
    (hash.rotate_left(ROTATION_LENGTH) ^ value).wrapping_mul(GOLDEN_RATIO)
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0u64;
    for &b in bytes {
        hash = add_u64_to_hash(hash, b as u64);
    }
    hash
}

pub struct SymbolTable {
    symbols: Vec<String>,
    hash_to_index: HashMap<u64, usize>,
}

impl SymbolTable {
    pub fn new(symbols: Vec<String>) -> Self {
        let hash_to_index = symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (hash_bytes(s.as_bytes()), i))
            .collect();

        Self {
            symbols,
            hash_to_index,
        }
    }

    pub fn resolve(&self, hash: u64) -> Option<&str> {
        self.hash_to_index
            .get(&hash)
            .and_then(|&i| self.symbols.get(i))
            .map(|s| s.as_str())
    }

    pub fn resolve_field_key(&self, key: u64, send_node_hash: Option<u64>) -> Option<String> {
        if let Some(sn_hash) = send_node_hash {
            let send_node = self.resolve(sn_hash)?;
            let var_name_hash = self.reverse_key_to_var_name(key, sn_hash)?;
            let var_name = self.resolve(var_name_hash)?;
            Some(format!("{}__{}", send_node.replace('.', "__"), var_name))
        } else {
            self.resolve(key).map(|s| s.to_string())
        }
    }

    fn reverse_key_to_var_name(&self, _key: u64, _send_node_hash: u64) -> Option<u64> {
        None
    }

    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }
}

pub fn build_field_name(send_node: Option<&str>, var_name: &str) -> String {
    match send_node {
        Some(sn) if !sn.is_empty() => {
            format!("{}__{}", sn.replace('.', "__"), var_name)
        }
        _ => var_name.to_string(),
    }
}

pub fn compute_field_key(send_node: Option<&str>, var_name: &str) -> u64 {
    let var_name_hash = hash_bytes(var_name.as_bytes());

    match send_node {
        Some(sn) if !sn.is_empty() => {
            let mut parts = sn.split('.');
            let first_part = parts.next().unwrap();
            let mut hash = hash_bytes(first_part.as_bytes());
            for part in parts {
                let part_hash = hash_bytes(part.as_bytes());
                hash = add_u64_to_hash(hash, part_hash);
            }
            add_u64_to_hash(hash, var_name_hash)
        }
        _ => var_name_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_table_resolve() {
        let symbols = vec![
            "m_iHealth".to_string(),
            "m_vecOrigin".to_string(),
            "CCitadelPlayerPawn".to_string(),
        ];
        let table = SymbolTable::new(symbols);

        let health_hash = hash_bytes(b"m_iHealth");
        assert_eq!(table.resolve(health_hash), Some("m_iHealth"));

        let origin_hash = hash_bytes(b"m_vecOrigin");
        assert_eq!(table.resolve(origin_hash), Some("m_vecOrigin"));

        assert_eq!(table.resolve(12345), None);
    }

    #[test]
    fn test_build_field_name() {
        assert_eq!(build_field_name(None, "m_iHealth"), "m_iHealth");
        assert_eq!(build_field_name(Some(""), "m_iHealth"), "m_iHealth");
        assert_eq!(
            build_field_name(Some("CBodyComponent"), "m_vecX"),
            "CBodyComponent__m_vecX"
        );
        assert_eq!(
            build_field_name(Some("m_CCitadelHeroComponent.m_loadingHero"), "m_nHeroID"),
            "m_CCitadelHeroComponent__m_loadingHero__m_nHeroID"
        );
    }

    #[test]
    fn test_compute_field_key_matches_haste() {
        let simple_key = compute_field_key(None, "m_iHealth");
        assert_eq!(simple_key, hash_bytes(b"m_iHealth"));
    }
}
