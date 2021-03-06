use datafusion::{
    error::{DataFusionError, Result},
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, Operator},
    scalar::ScalarValue,
};

use substrait::protobuf::{
    expression::{
        field_reference::ReferenceType,
        literal::LiteralType,
        mask_expression::{StructItem, StructSelect},
        FieldReference, Literal, MaskExpression, RexType, ScalarFunction,
    },
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    Expression, FilterRel, NamedStruct, ProjectRel, ReadRel, Rel,
};

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

            if let Some(struct_items) = projection {
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
                            select: Some(StructSelect { struct_items }),
                            maintain_singular_struct: false,
                        }),
                        advanced_extension: None,
                        read_type: Some(ReadType::NamedTable(NamedTable {
                            names: vec![scan.table_name.clone()],
                            advanced_extension: None,
                        })),
                    }))),
                }))
            } else {
                Err(DataFusionError::NotImplemented(
                    "TableScan without projection is not supported".to_string(),
                ))
            }
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

pub fn operator_to_reference(op: Operator) -> u32 {
    match op {
        Operator::Eq => 1,
        Operator::NotEq => 2,
        Operator::Lt => 3,
        Operator::LtEq => 4,
        Operator::Gt => 5,
        Operator::GtEq => 6,
        Operator::Plus => 7,
        Operator::Minus => 8,
        Operator::Multiply => 9,
        Operator::Divide => 10,
        Operator::Modulo => 11,
        Operator::And => 12,
        Operator::Or => 13,
        Operator::Like => 14,
        Operator::NotLike => 15,
        Operator::IsDistinctFrom => 16,
        Operator::IsNotDistinctFrom => 17,
        Operator::RegexMatch => 18,
        Operator::RegexIMatch => 19,
        Operator::RegexNotMatch => 20,
        Operator::RegexNotIMatch => 21,
        Operator::BitwiseAnd => 22,
        Operator::BitwiseOr => 23,
        Operator::StringConcat => 24,
    }
}

/// Convert DataFusion Expr to Substrait Rex
pub fn to_substrait_rex(expr: &Expr, schema: &DFSchemaRef) -> Result<Expression> {
    match expr {
        Expr::Column(col) => Ok(Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::MaskedReference(MaskExpression {
                    select: Some(StructSelect {
                        struct_items: vec![StructItem {
                            field: schema.index_of_column_by_name(None, &col.name)? as i32,
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
            let function_reference: u32 = operator_to_reference(*op);
            Ok(Expression {
                rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                    function_reference,
                    args: vec![l, r],
                    output_type: None,
                })),
            })
        }
        Expr::Literal(value) => {
            let literal_type = match value {
                ScalarValue::Int8(Some(n)) => Some(LiteralType::I8(*n as i32)),
                ScalarValue::Int16(Some(n)) => Some(LiteralType::I16(*n as i32)),
                ScalarValue::Int32(Some(n)) => Some(LiteralType::I32(*n)),
                ScalarValue::Int64(Some(n)) => Some(LiteralType::I64(*n)),
                ScalarValue::Boolean(Some(b)) => Some(LiteralType::Boolean(*b)),
                ScalarValue::Float32(Some(f)) => Some(LiteralType::Fp32(*f)),
                ScalarValue::Float64(Some(f)) => Some(LiteralType::Fp64(*f)),
                ScalarValue::Utf8(Some(s)) => Some(LiteralType::String(s.clone())),
                ScalarValue::LargeUtf8(Some(s)) => Some(LiteralType::String(s.clone())),
                ScalarValue::Binary(Some(b)) => Some(LiteralType::Binary(b.clone())),
                ScalarValue::LargeBinary(Some(b)) => Some(LiteralType::Binary(b.clone())),
                ScalarValue::Date32(Some(d)) => Some(LiteralType::Date(*d)),
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported literal: {:?}",
                        value
                    )))
                }
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
