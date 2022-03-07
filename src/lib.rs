use std::sync::Arc;

use async_recursion::async_recursion;

use datafusion::prelude::{DataFrame, ExecutionContext};
use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, Operator},
    prelude::Column,
    scalar::ScalarValue,
};

use substrait::protobuf::{
    derivation_expression::{binary_op, BinaryOp},
    expression::{
        field_reference::{ReferenceType, ReferenceType::MaskedReference},
        literal::LiteralType,
        mask_expression::{StructItem, StructSelect},
        FieldReference, Literal, MaskExpression, RexType,
    },
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    Expression, FilterRel, NamedStruct, ProjectRel, ReadRel, Rel,
};

/// Convert DataFusion Expr to Substrait Rex
pub fn to_substrait_rex(expr: &Expr, schema: &DFSchemaRef) -> Result<Expression> {
    match expr {
        Expr::Column(col) => Ok(Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::MaskedReference(MaskExpression {
                    select: Some(StructSelect {
                        struct_items: vec![StructItem {
                            field: schema.index_of(&col.name)? as i32,
                            child: None,
                        }],
                    }),
                    maintain_singular_struct: false,
                })),
                root_type: None,
            }))),
        }),
        Expr::BinaryExpr { left, op, right } => {
            let l = to_substrait_rex(left, schema)?;
            let r = to_substrait_rex(right, schema)?;
            // let op_type = match op {
            //     Operator::Lt => Ok(binary_op::BinaryOpType::LessThan),
            //     Operator::Gt => Ok(binary_op::BinaryOpType::GreaterThan),
            //     _ => Err(DataFusionError::NotImplemented(format!(
            //         "Unsupported binary operator '{}'",
            //         op
            //     ))),
            // }?;
            // let _ = BinaryOp {
            //     arg1: None,
            //     arg2: None,
            //     op_type: op_type as i32,
            // };
            todo!()
        }
        Expr::Literal(value) => {
            let literal_type = match value {
                ScalarValue::Int8(Some(n)) => Some(LiteralType::I8(*n as i32)),
                ScalarValue::Int16(Some(n)) => Some(LiteralType::I16(*n as i32)),
                ScalarValue::Int32(Some(n)) => Some(LiteralType::I32(*n)),
                ScalarValue::Int64(Some(n)) => Some(LiteralType::I64(*n)),
                _ => todo!(),
            };
            Ok(Expression {
                rex_type: Some(RexType::Literal(Literal {
                    nullable: true,
                    literal_type,
                })),
            })
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported expression: {:?}",
            expr
        ))),
    }
}

/// Convert DataFusion LogicalPlan to Substrait Rel
pub fn to_substrait_rel(plan: &LogicalPlan) -> Result<Box<Rel>> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let projection = scan.projection.as_ref().map(|p| {
                p.iter()
                    .map(|i| StructItem {
                        field: *i as i32,
                        child: None,
                    })
                    .collect()
            });

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,
                    base_schema: Some(NamedStruct {
                        names: scan
                            .projected_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().to_owned())
                            .collect(),
                        r#struct: None,
                    }),
                    filter: None,
                    projection: Some(MaskExpression {
                        select: Some(StructSelect {
                            struct_items: projection.unwrap(),
                        }),
                        maintain_singular_struct: false,
                    }),
                    advanced_extension: None,
                    read_type: Some(ReadType::NamedTable(NamedTable {
                        names: vec![scan.table_name.clone()],
                        advanced_extension: None,
                    })),
                }))),
            }))
        }
        LogicalPlan::Projection(p) => {
            let expressions = p
                .expr
                .iter()
                .map(|e| to_substrait_rex(e, p.input.schema()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(Box::new(ProjectRel {
                    common: None,
                    input: Some(to_substrait_rel(p.input.as_ref())?),
                    expressions,
                    advanced_extension: None,
                }))),
            }))
        }
        LogicalPlan::Filter(filter) => {
            let input = to_substrait_rel(filter.input.as_ref())?;
            let filter_expr = to_substrait_rex(&filter.predicate, filter.input.schema())?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Filter(Box::new(FilterRel {
                    common: None,
                    input: Some(input),
                    condition: Some(Box::new(filter_expr)),
                    advanced_extension: None,
                }))),
            }))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported operator: {:?}",
            plan
        ))),
    }
}

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    ctx: &mut ExecutionContext,
    rel: &Rel,
) -> Result<Arc<dyn DataFrame>> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            let input = from_substrait_rel(ctx, p.input.as_ref().unwrap()).await?;
            let exprs: Vec<Expr> = p
                .expressions
                .iter()
                .map(|e| match &e.rex_type {
                    Some(RexType::Selection(field_ref)) => match &field_ref.reference_type {
                        Some(MaskedReference(mask)) => match &mask.select.as_ref() {
                            Some(x) if x.struct_items.len() == 1 => Ok(Expr::Column(Column {
                                relation: None,
                                name: input
                                    .schema()
                                    .field(x.struct_items[0].field as usize)
                                    .name()
                                    .to_string(),
                            })),
                            _ => Err(DataFusionError::NotImplemented(
                                "invalid field reference".to_string(),
                            )),
                        },
                        _ => Err(DataFusionError::NotImplemented(
                            "unsupported field ref type".to_string(),
                        )),
                    },
                    _ => Err(DataFusionError::NotImplemented(
                        "unsupported rex_type in projection".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;

            input.select(exprs)
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_name: String = nt.names[0].clone();
                ctx.table(&*table_name)
            }
            _ => Err(DataFusionError::NotImplemented(
                "Only NamedTable reads are supported".to_string(),
            )),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported RelType: {:?}",
            rel.rel_type
        ))),
    }
}

#[cfg(test)]
mod tests {

    use crate::{from_substrait_rel, to_substrait_rel};
    use datafusion::error::Result;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn simple_select() -> Result<()> {
        roundtrip("SELECT a, b FROM data").await
    }

    #[tokio::test]
    async fn wildcard_select() -> Result<()> {
        roundtrip("SELECT * FROM data").await
    }

    #[tokio::test]
    async fn select_with_filter() -> Result<()> {
        roundtrip("SELECT * FROM data WHERE a > 1").await
    }

    async fn roundtrip(sql: &str) -> Result<()> {
        let mut ctx = ExecutionContext::new();
        ctx.register_csv("data", "testdata/data.csv", CsvReadOptions::new())
            .await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan();
        let proto = to_substrait_rel(&plan)?;
        let df = from_substrait_rel(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan();
        let plan2 = ctx.optimize(&plan2)?;
        let plan1str = format!("{:?}", plan);
        let plan2str = format!("{:?}", plan2);
        assert_eq!(plan1str, plan2str);
        println!("{}", plan2str);
        Ok(())
    }
}
