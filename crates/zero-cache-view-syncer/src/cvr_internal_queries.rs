//! Port of `getMutationResultsQuery` (and the `CLIENT_*_QUERY_ID` constants)
//! from `zero-cache/src/services/view-syncer/cvr.ts`.

use zero_cache_protocol::ast::{
    Ast, ColumnReference, Condition, Direction, LiteralValue, SimpleOperator, ValuePosition,
};

use crate::cvr_types::InternalQueryRecord;

/// The reserved query id for the last-mutation-ID internal query.
pub const CLIENT_LMID_QUERY_ID: &str = "lmids";
/// The reserved query id for the mutation-results internal query.
pub const CLIENT_MUTATION_RESULTS_QUERY_ID: &str = "mutationResults";

/// Asserts `query` is not internal, i.e. that its id is not one of the
/// reserved internal query ids. Port of `assertNotInternal`. Panics
/// (mirroring the TS `throw`) if it is internal.
pub fn assert_not_internal(query_id: &str, query_type_is_internal: bool) {
    if query_type_is_internal {
        panic!("Query ID {query_id} is reserved for internal use");
    }
}

/// Builds the internal query that fetches mutation results for a client
/// group: `SELECT * FROM {upstreamSchema}.mutations WHERE clientGroupID = ?
/// ORDER BY clientGroupID, clientID, mutationID`. Port of
/// `getMutationResultsQuery`.
pub fn get_mutation_results_query(
    upstream_schema: &str,
    client_group_id: &str,
) -> InternalQueryRecord {
    let ast = Ast {
        schema: Some(String::new()),
        table: format!("{upstream_schema}.mutations"),
        where_: Some(Condition::And {
            conditions: vec![Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "clientGroupID".into(),
                }),
                right: ValuePosition::Literal(LiteralValue::String(client_group_id.to_string())),
            }],
        }),
        order_by: Some(vec![
            ("clientGroupID".into(), Direction::Asc),
            ("clientID".into(), Direction::Asc),
            ("mutationID".into(), Direction::Asc),
        ]),
        ..Default::default()
    };

    InternalQueryRecord {
        id: CLIENT_MUTATION_RESULTS_QUERY_ID.to_string(),
        transformation_hash: None,
        transformation_version: None,
        row_set_signature: None,
        ast,
    }
}

/// Builds the internal "last mutation ID" tracking query for a client group:
/// `SELECT * FROM {upstreamSchema}.clients WHERE clientGroupID = ? ORDER BY
/// clientGroupID, clientID`. Port of the `lmidsQuery` construction inline in
/// `CVRConfigDrivenUpdater.ensureClient`.
pub fn get_lmids_query(upstream_clients_schema: &str, cvr_id: &str) -> InternalQueryRecord {
    let ast = Ast {
        schema: Some(String::new()),
        table: format!("{upstream_clients_schema}.clients"),
        where_: Some(Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "clientGroupID".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::String(cvr_id.to_string())),
        }),
        order_by: Some(vec![
            ("clientGroupID".into(), Direction::Asc),
            ("clientID".into(), Direction::Asc),
        ]),
        ..Default::default()
    };

    InternalQueryRecord {
        id: CLIENT_LMID_QUERY_ID.to_string(),
        transformation_hash: None,
        transformation_version: None,
        row_set_signature: None,
        ast,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_lmids_query() {
        let record = get_lmids_query("zero_0", "cvr1");
        assert_eq!(record.id, "lmids");
        assert_eq!(record.ast.table, "zero_0.clients");
        assert_eq!(
            record.ast.where_,
            Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "clientGroupID".into()
                }),
                right: ValuePosition::Literal(LiteralValue::String("cvr1".into())),
            })
        );
        assert_eq!(
            record.ast.order_by,
            Some(vec![
                ("clientGroupID".into(), Direction::Asc),
                ("clientID".into(), Direction::Asc),
            ])
        );
    }

    #[test]
    fn builds_expected_ast() {
        let record = get_mutation_results_query("zero_0", "cg1");
        assert_eq!(record.id, "mutationResults");
        assert_eq!(record.ast.schema, Some(String::new()));
        assert_eq!(record.ast.table, "zero_0.mutations");

        let Some(Condition::And { conditions }) = &record.ast.where_ else {
            panic!("expected AND condition");
        };
        assert_eq!(conditions.len(), 1);
        match &conditions[0] {
            Condition::Simple { op, left, right } => {
                assert_eq!(*op, SimpleOperator::Eq);
                assert_eq!(
                    left,
                    &ValuePosition::Column(ColumnReference {
                        name: "clientGroupID".into()
                    })
                );
                assert_eq!(
                    right,
                    &ValuePosition::Literal(LiteralValue::String("cg1".into()))
                );
            }
            _ => panic!("expected simple condition"),
        }

        assert_eq!(
            record.ast.order_by,
            Some(vec![
                ("clientGroupID".into(), Direction::Asc),
                ("clientID".into(), Direction::Asc),
                ("mutationID".into(), Direction::Asc),
            ])
        );
    }

    #[test]
    #[should_panic(expected = "reserved for internal use")]
    fn assert_not_internal_panics_on_internal() {
        assert_not_internal(CLIENT_LMID_QUERY_ID, true);
    }

    #[test]
    fn assert_not_internal_passes_for_client_query() {
        assert_not_internal("some-client-query-hash", false);
    }
}
