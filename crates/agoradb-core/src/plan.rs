use crate::SpaceUri;

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    Scan {
        space: SpaceUri,
        projection: Option<Vec<usize>>,
        filter: Option<PhysicalExpr>,
    },
    Filter {
        predicate: PhysicalExpr,
        input: Box<PhysicalPlan>,
    },
    Project {
        expressions: Vec<PhysicalExpr>,
        input: Box<PhysicalPlan>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        left_key: usize,
        right_key: usize,
        join_type: JoinType,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_exprs: Vec<PhysicalExpr>,
        agg_exprs: Vec<(PhysicalExpr, AggFunction)>,
    },
    Limit {
        skip: usize,
        fetch: usize,
        input: Box<PhysicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalExpr {
    Column(usize),
    Literal(i64),
    BinaryOp {
        op: BinaryOp,
        left: Box<PhysicalExpr>,
        right: Box<PhysicalExpr>,
    },
}

#[derive(Debug, Clone)]
pub enum BinaryOp {
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}
