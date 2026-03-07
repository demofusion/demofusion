//! Analyze DataFusion Expr filters and extract pushdown-eligible predicates.
//!
//! Produces a DemoScanFilters with:
//!   - tick_min / tick_max (for early stop)
//!   - entity_index filter (skip non-matching entities in visitor)

use datafusion::logical_expr::{Expr, Operator};
use datafusion::scalar::ScalarValue;
use std::collections::HashSet;

#[derive(Debug, Clone, Default)]
pub struct DemoScanFilters {
    pub tick_min: Option<i32>,
    pub tick_max: Option<i32>,
    pub entity_indices: Option<HashSet<i32>>,
}

impl DemoScanFilters {
    pub fn has_tick_bounds(&self) -> bool {
        self.tick_min.is_some() || self.tick_max.is_some()
    }

    pub fn has_entity_filter(&self) -> bool {
        self.entity_indices.is_some()
    }

    pub fn should_include_tick(&self, tick: i32) -> bool {
        if let Some(min) = self.tick_min {
            if tick < min {
                return false;
            }
        }
        if let Some(max) = self.tick_max {
            if tick > max {
                return false;
            }
        }
        true
    }

    pub fn exceeds_max_tick(&self, tick: i32) -> bool {
        self.tick_max.is_some_and(|max| tick > max)
    }

    pub fn should_include_entity(&self, entity_index: i32) -> bool {
        self.entity_indices
            .as_ref()
            .is_none_or(|indices| indices.contains(&entity_index))
    }

    pub fn merge(&mut self, other: &DemoScanFilters) {
        if let Some(other_min) = other.tick_min {
            self.tick_min = Some(self.tick_min.map_or(other_min, |m| m.max(other_min)));
        }
        if let Some(other_max) = other.tick_max {
            self.tick_max = Some(self.tick_max.map_or(other_max, |m| m.min(other_max)));
        }
        if let Some(ref other_indices) = other.entity_indices {
            match &mut self.entity_indices {
                Some(indices) => {
                    *indices = indices.intersection(other_indices).copied().collect();
                }
                None => {
                    self.entity_indices = Some(other_indices.clone());
                }
            }
        }
    }
}

pub fn extract_filters(exprs: &[Expr]) -> DemoScanFilters {
    let mut filters = DemoScanFilters::default();
    for expr in exprs {
        extract_from_expr(expr, &mut filters);
    }
    filters
}

fn extract_from_expr(expr: &Expr, filters: &mut DemoScanFilters) {
    match expr {
        Expr::BinaryExpr(binary) => {
            extract_from_binary(&binary.left, binary.op, &binary.right, filters);
        }
        Expr::Between(between) => {
            if let Some(col_name) = extract_column_name(&between.expr) {
                if col_name == "tick" {
                    if let Some(low) = extract_i32_value(&between.low) {
                        let effective_min = if between.negated { low + 1 } else { low };
                        filters.tick_min = Some(
                            filters
                                .tick_min
                                .map_or(effective_min, |m| m.max(effective_min)),
                        );
                    }
                    if let Some(high) = extract_i32_value(&between.high) {
                        let effective_max = if between.negated { high - 1 } else { high };
                        filters.tick_max = Some(
                            filters
                                .tick_max
                                .map_or(effective_max, |m| m.min(effective_max)),
                        );
                    }
                }
            }
        }
        Expr::InList(in_list) => {
            if let Some(col_name) = extract_column_name(&in_list.expr) {
                if col_name == "entity_index" && !in_list.negated {
                    let indices: HashSet<i32> =
                        in_list.list.iter().filter_map(extract_i32_value).collect();
                    if !indices.is_empty() {
                        match &mut filters.entity_indices {
                            Some(existing) => {
                                *existing = existing.intersection(&indices).copied().collect();
                            }
                            None => {
                                filters.entity_indices = Some(indices);
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn extract_from_binary(left: &Expr, op: Operator, right: &Expr, filters: &mut DemoScanFilters) {
    match op {
        Operator::And => {
            extract_from_expr(left, filters);
            extract_from_expr(right, filters);
        }
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
            extract_comparison(left, op, right, filters);
        }
        _ => {}
    }
}

fn extract_comparison(left: &Expr, op: Operator, right: &Expr, filters: &mut DemoScanFilters) {
    let (col_name, value, op) = match (extract_column_name(left), extract_column_name(right)) {
        (Some(name), None) => {
            if let Some(v) = extract_i32_value(right) {
                (name, v, op)
            } else {
                return;
            }
        }
        (None, Some(name)) => {
            if let Some(v) = extract_i32_value(left) {
                (name, v, flip_operator(op))
            } else {
                return;
            }
        }
        _ => return,
    };

    match col_name.as_str() {
        "tick" => apply_tick_filter(op, value, filters),
        "entity_index" => {
            if op == Operator::Eq {
                let mut indices = HashSet::new();
                indices.insert(value);
                match &mut filters.entity_indices {
                    Some(existing) => {
                        *existing = existing.intersection(&indices).copied().collect();
                    }
                    None => {
                        filters.entity_indices = Some(indices);
                    }
                }
            }
        }
        _ => {}
    }
}

fn apply_tick_filter(op: Operator, value: i32, filters: &mut DemoScanFilters) {
    match op {
        Operator::Eq => {
            filters.tick_min = Some(filters.tick_min.map_or(value, |m| m.max(value)));
            filters.tick_max = Some(filters.tick_max.map_or(value, |m| m.min(value)));
        }
        Operator::Lt => {
            let max = value - 1;
            filters.tick_max = Some(filters.tick_max.map_or(max, |m| m.min(max)));
        }
        Operator::LtEq => {
            filters.tick_max = Some(filters.tick_max.map_or(value, |m| m.min(value)));
        }
        Operator::Gt => {
            let min = value + 1;
            filters.tick_min = Some(filters.tick_min.map_or(min, |m| m.max(min)));
        }
        Operator::GtEq => {
            filters.tick_min = Some(filters.tick_min.map_or(value, |m| m.max(value)));
        }
        _ => {}
    }
}

fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::Cast(cast) => extract_column_name(&cast.expr),
        _ => None,
    }
}

fn extract_i32_value(expr: &Expr) -> Option<i32> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_i32(scalar),
        Expr::Cast(cast) => extract_i32_value(&cast.expr),
        _ => None,
    }
}

fn scalar_to_i32(scalar: &ScalarValue) -> Option<i32> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(*v as i32),
        ScalarValue::Int16(Some(v)) => Some(*v as i32),
        ScalarValue::Int32(Some(v)) => Some(*v),
        ScalarValue::Int64(Some(v)) => (*v).try_into().ok(),
        ScalarValue::UInt8(Some(v)) => Some(*v as i32),
        ScalarValue::UInt16(Some(v)) => Some(*v as i32),
        ScalarValue::UInt32(Some(v)) => (*v).try_into().ok(),
        ScalarValue::UInt64(Some(v)) => (*v).try_into().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::expr::InList;

    fn tick_col() -> Expr {
        Expr::Column(Column::new_unqualified("tick"))
    }

    fn entity_index_col() -> Expr {
        Expr::Column(Column::new_unqualified("entity_index"))
    }

    fn lit_i32(v: i32) -> Expr {
        Expr::Literal(ScalarValue::Int32(Some(v)), None)
    }

    #[test]
    fn extract_tick_less_than_or_equal() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(tick_col()),
            Operator::LtEq,
            Box::new(lit_i32(100000)),
        ));
        let filters = extract_filters(&[expr]);
        assert_eq!(filters.tick_max, Some(100000));
        assert_eq!(filters.tick_min, None);
    }

    #[test]
    fn extract_tick_greater_than() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(tick_col()),
            Operator::Gt,
            Box::new(lit_i32(5000)),
        ));
        let filters = extract_filters(&[expr]);
        assert_eq!(filters.tick_min, Some(5001));
        assert_eq!(filters.tick_max, None);
    }

    #[test]
    fn extract_tick_range() {
        let expr1 = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(tick_col()),
            Operator::GtEq,
            Box::new(lit_i32(1000)),
        ));
        let expr2 = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(tick_col()),
            Operator::Lt,
            Box::new(lit_i32(2000)),
        ));
        let filters = extract_filters(&[expr1, expr2]);
        assert_eq!(filters.tick_min, Some(1000));
        assert_eq!(filters.tick_max, Some(1999));
    }

    #[test]
    fn extract_entity_index_equality() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(entity_index_col()),
            Operator::Eq,
            Box::new(lit_i32(42)),
        ));
        let filters = extract_filters(&[expr]);
        assert!(filters.entity_indices.as_ref().unwrap().contains(&42));
        assert_eq!(filters.entity_indices.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn extract_entity_index_in_list() {
        let expr = Expr::InList(InList::new(
            Box::new(entity_index_col()),
            vec![lit_i32(1), lit_i32(2), lit_i32(3)],
            false,
        ));
        let filters = extract_filters(&[expr]);
        let indices = filters.entity_indices.unwrap();
        assert!(indices.contains(&1));
        assert!(indices.contains(&2));
        assert!(indices.contains(&3));
        assert_eq!(indices.len(), 3);
    }

    #[test]
    fn reversed_comparison() {
        let expr = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(lit_i32(50000)),
            Operator::GtEq,
            Box::new(tick_col()),
        ));
        let filters = extract_filters(&[expr]);
        assert_eq!(filters.tick_max, Some(50000));
    }

    #[test]
    fn should_include_tick_within_bounds() {
        let mut filters = DemoScanFilters::default();
        filters.tick_min = Some(100);
        filters.tick_max = Some(200);

        assert!(!filters.should_include_tick(99));
        assert!(filters.should_include_tick(100));
        assert!(filters.should_include_tick(150));
        assert!(filters.should_include_tick(200));
        assert!(!filters.should_include_tick(201));
    }

    #[test]
    fn exceeds_max_tick_check() {
        let mut filters = DemoScanFilters::default();
        filters.tick_max = Some(100);

        assert!(!filters.exceeds_max_tick(99));
        assert!(!filters.exceeds_max_tick(100));
        assert!(filters.exceeds_max_tick(101));
    }
}
