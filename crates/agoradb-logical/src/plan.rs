#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        schema: Vec<(String, DataType)>,
    },
    Filter {
        predicate: LogicalExpr,
        input: Box<LogicalPlan>,
    },
    Project {
        expressions: Vec<(String, LogicalExpr)>,
        input: Box<LogicalPlan>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: LogicalExpr,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<(String, AggFunction, LogicalExpr)>,
    },
    Limit {
        skip: usize,
        fetch: usize,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(String),
    Literal(LiteralValue),
    BinaryOp {
        op: BinaryOp,
        left: Box<LogicalExpr>,
        right: Box<LogicalExpr>,
    },
    Function {
        name: String,
        args: Vec<LogicalExpr>,
    },
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    String(String),
    Null,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Int64,
    Float64,
    Boolean,
    Utf8,
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
    Add,
    Sub,
    Mul,
    Div,
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
