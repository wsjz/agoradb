use agoradb_sql::parser::SqlParser;

#[test]
fn test_parse_select_from() {
    let parser = SqlParser::new();
    let plan = parser.parse("SELECT id, name FROM users").unwrap();

    // Should produce: Project([id, name] → Scan(users))
    match plan {
        agoradb_logical::plan::LogicalPlan::Project { expressions, input } => {
            assert_eq!(expressions.len(), 2);
            assert_eq!(expressions[0].0, "id");
            assert_eq!(expressions[1].0, "name");
            match input.as_ref() {
                agoradb_logical::plan::LogicalPlan::Scan { table, .. } => {
                    assert_eq!(table, "users");
                }
                _ => panic!("Expected Scan, got {:?}", input),
            }
        }
        _ => panic!("Expected Project, got {:?}", plan),
    }
}

#[test]
fn test_parse_select_where() {
    let parser = SqlParser::new();
    let plan = parser.parse("SELECT id FROM users WHERE id > 10").unwrap();

    // Should produce: Project([id] → Filter(id > 10) → Scan(users))
    match plan {
        agoradb_logical::plan::LogicalPlan::Project { input, .. } => match input.as_ref() {
            agoradb_logical::plan::LogicalPlan::Filter { predicate, input } => {
                match predicate {
                    agoradb_logical::plan::LogicalExpr::BinaryOp { op, .. } => {
                        assert!(matches!(op, agoradb_logical::plan::BinaryOp::Gt));
                    }
                    _ => panic!("Expected binary op, got {:?}", predicate),
                }
                match input.as_ref() {
                    agoradb_logical::plan::LogicalPlan::Scan { table, .. } => {
                        assert_eq!(table, "users");
                    }
                    _ => panic!("Expected Scan, got {:?}", input),
                }
            }
            _ => panic!("Expected Filter, got {:?}", input),
        },
        _ => panic!("Expected Project, got {:?}", plan),
    }
}
