//! Pipeline analysis for detecting operators that break streaming execution.
//!
//! DataFusion's physical plans have operators with different emission behaviors:
//! - `EmissionType::Incremental` - Emits rows as they arrive (streaming-friendly)
//! - `EmissionType::Final` - Must see all input before emitting (pipeline breaker)
//! - `EmissionType::Both` - Can do either depending on configuration
//!
//! For streaming queries over unbounded sources (GOTV broadcasts), pipeline breakers
//! cause the entire stream to buffer until the source closes, defeating the purpose
//! of streaming. This module detects these operators before execution so callers
//! can warn users or reject problematic queries.
//!
//! # Example
//!
//! ```ignore
//! let physical_plan = ctx.state().create_physical_plan(&logical).await?;
//! let analysis = analyze_pipeline(&physical_plan);
//!
//! if analysis.has_pipeline_breakers() {
//!     eprintln!("Warning: Query contains pipeline breakers:");
//!     for breaker in &analysis.breakers {
//!         eprintln!("  - {} at depth {}", breaker.operator_name, breaker.depth);
//!     }
//! }
//! ```

use std::sync::Arc;

use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

/// Information about a single pipeline breaker in the plan tree.
#[derive(Debug, Clone)]
pub struct PipelineBreaker {
    /// Name of the operator (e.g., "SortExec", "HashAggregateExec")
    pub operator_name: String,

    /// Depth in the plan tree (0 = root)
    pub depth: usize,

    /// The emission type of this operator
    pub emission_type: EmissionType,

    /// Human-readable path to this node (e.g., "ProjectionExec -> SortExec")
    pub path: String,
}

/// Result of analyzing a physical plan for streaming compatibility.
#[derive(Debug, Clone)]
pub struct PipelineAnalysis {
    /// All pipeline breakers found in the plan tree
    pub breakers: Vec<PipelineBreaker>,

    /// Total number of operators analyzed
    pub total_operators: usize,
}

impl PipelineAnalysis {
    /// Returns true if any pipeline breakers were found.
    pub fn has_pipeline_breakers(&self) -> bool {
        !self.breakers.is_empty()
    }

    /// Returns true if the plan is fully streaming-compatible.
    pub fn is_streaming_safe(&self) -> bool {
        self.breakers.is_empty()
    }

    /// Format a human-readable report of the analysis.
    pub fn report(&self) -> String {
        if self.breakers.is_empty() {
            return format!(
                "Pipeline analysis: OK ({} operators, all streaming-compatible)",
                self.total_operators
            );
        }

        let mut lines = vec![format!(
            "Pipeline analysis: FOUND {} PIPELINE BREAKER(S) in {} operators",
            self.breakers.len(),
            self.total_operators
        )];

        for breaker in &self.breakers {
            lines.push(format!(
                "  [BREAKER] {} (depth={}, emission={:?})",
                breaker.operator_name, breaker.depth, breaker.emission_type
            ));
            lines.push(format!("            path: {}", breaker.path));
        }

        lines.push(String::new());
        lines.push("Pipeline breakers buffer all input before producing output.".to_string());
        lines.push("For streaming queries, data won't flow until the source closes.".to_string());

        lines.join("\n")
    }
}

/// Analyze a physical plan for pipeline breakers.
///
/// Walks the entire plan tree and identifies operators that are *root cause*
/// pipeline breakers - operators with `EmissionType::Final` whose children
/// are NOT all `Final`. This filters out operators that are only `Final`
/// because they inherit blocking behavior from children.
pub fn analyze_pipeline(plan: &Arc<dyn ExecutionPlan>) -> PipelineAnalysis {
    let mut breakers = Vec::new();
    let mut total_operators = 0;

    walk_plan(plan, 0, String::new(), &mut breakers, &mut total_operators);

    PipelineAnalysis {
        breakers,
        total_operators,
    }
}

fn walk_plan(
    plan: &Arc<dyn ExecutionPlan>,
    depth: usize,
    parent_path: String,
    breakers: &mut Vec<PipelineBreaker>,
    total_operators: &mut usize,
) {
    *total_operators += 1;

    let name = plan.name().to_string();
    let current_path = if parent_path.is_empty() {
        name.clone()
    } else {
        format!("{} -> {}", parent_path, name)
    };

    let emission_type = plan.pipeline_behavior();

    // An operator is a *root cause* pipeline breaker if:
    // 1. It has EmissionType::Final, AND
    // 2. At least one of its children is NOT Final (or it has no children)
    //
    // This filters out operators that are only Final because their children are Final
    // (e.g., ProjectionExec over SortExec reports Final, but Projection isn't the cause)
    if matches!(emission_type, EmissionType::Final) {
        let children = plan.children();
        let all_children_final = !children.is_empty()
            && children
                .iter()
                .all(|c| matches!(c.pipeline_behavior(), EmissionType::Final));

        if !all_children_final {
            breakers.push(PipelineBreaker {
                operator_name: name.clone(),
                depth,
                emission_type,
                path: current_path.clone(),
            });
        }
    }

    for child in plan.children() {
        walk_plan(
            child,
            depth + 1,
            current_path.clone(),
            breakers,
            total_operators,
        );
    }
}

/// Check if a plan is streaming-safe and return an error if not.
///
/// This is a convenience function for callers who want to reject
/// plans with pipeline breakers rather than just warn about them.
pub fn require_streaming_safe(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<PipelineAnalysis, PipelineAnalysisError> {
    let analysis = analyze_pipeline(plan);

    if analysis.has_pipeline_breakers() {
        Err(PipelineAnalysisError::PipelineBreakersFound(analysis))
    } else {
        Ok(analysis)
    }
}

/// Error returned when a plan is not streaming-safe.
#[derive(Debug)]
pub enum PipelineAnalysisError {
    /// The plan contains one or more pipeline breakers.
    PipelineBreakersFound(PipelineAnalysis),
}

impl std::fmt::Display for PipelineAnalysisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipelineAnalysisError::PipelineBreakersFound(analysis) => {
                write!(f, "{}", analysis.report())
            }
        }
    }
}

impl std::error::Error for PipelineAnalysisError {}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::arrow::compute::SortOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::sorts::sort::SortExec;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn make_lex_ordering(exprs: Vec<PhysicalSortExpr>) -> LexOrdering {
        LexOrdering::new(exprs).expect("non-empty sort expressions")
    }

    #[test]
    fn test_memory_source_is_streaming_safe() {
        let schema = test_schema();
        let plan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();

        let analysis = analyze_pipeline(&plan);

        assert!(analysis.is_streaming_safe());
        assert!(!analysis.has_pipeline_breakers());
        assert_eq!(analysis.breakers.len(), 0);
        assert_eq!(analysis.total_operators, 1);
    }

    #[test]
    fn test_sort_is_pipeline_breaker() {
        let schema = test_schema();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap();

        let sort_expr = PhysicalSortExpr {
            expr: col("tick", &schema).unwrap(),
            options: SortOptions::default(),
        };
        let sort = SortExec::new(make_lex_ordering(vec![sort_expr]), source);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(sort);

        let analysis = analyze_pipeline(&plan);

        assert!(analysis.has_pipeline_breakers());
        assert!(!analysis.is_streaming_safe());
        assert_eq!(analysis.breakers.len(), 1);
        assert_eq!(analysis.breakers[0].operator_name, "SortExec");
        assert_eq!(analysis.breakers[0].depth, 0);
    }

    #[test]
    fn test_projection_is_streaming_safe() {
        let schema = test_schema();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap();

        let projection = ProjectionExec::try_new(
            vec![(col("tick", &schema).unwrap(), "tick".to_string())],
            source,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection);

        let analysis = analyze_pipeline(&plan);

        assert!(analysis.is_streaming_safe());
        assert_eq!(analysis.total_operators, 2); // ProjectionExec + DataSourceExec
    }

    #[test]
    fn test_nested_breaker_detected() {
        let schema = test_schema();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap();

        let sort_expr = PhysicalSortExpr {
            expr: col("tick", &schema).unwrap(),
            options: SortOptions::default(),
        };
        let sort: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(make_lex_ordering(vec![sort_expr]), source));

        let projection = ProjectionExec::try_new(
            vec![(col("tick", &schema).unwrap(), "tick".to_string())],
            sort,
        )
        .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(projection);

        let analysis = analyze_pipeline(&plan);

        assert!(analysis.has_pipeline_breakers());
        assert_eq!(analysis.breakers.len(), 1);
        assert_eq!(analysis.breakers[0].operator_name, "SortExec");
        assert_eq!(analysis.breakers[0].depth, 1); // One level deep
        assert!(analysis.breakers[0].path.contains("ProjectionExec"));
        assert!(analysis.breakers[0].path.contains("SortExec"));
    }

    #[test]
    fn test_report_format() {
        let schema = test_schema();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap();

        let sort_expr = PhysicalSortExpr {
            expr: col("tick", &schema).unwrap(),
            options: SortOptions::default(),
        };
        let sort = SortExec::new(make_lex_ordering(vec![sort_expr]), source);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(sort);

        let analysis = analyze_pipeline(&plan);
        let report = analysis.report();

        assert!(report.contains("PIPELINE BREAKER"));
        assert!(report.contains("SortExec"));
        assert!(report.contains("streaming"));
    }

    #[test]
    fn test_require_streaming_safe_ok() {
        let schema = test_schema();
        let plan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();

        let result = require_streaming_safe(&plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_require_streaming_safe_err() {
        let schema = test_schema();
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema.clone(), None).unwrap();

        let sort_expr = PhysicalSortExpr {
            expr: col("tick", &schema).unwrap(),
            options: SortOptions::default(),
        };
        let sort = SortExec::new(make_lex_ordering(vec![sort_expr]), source);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(sort);

        let result = require_streaming_safe(&plan);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("SortExec"));
    }
}
