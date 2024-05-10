use super::{Parser, ParserResult, Query};
use crate::{lexer::Token, types::Value, ParserError, Type};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Expression {
    Constant(Value),
    ColumnRef(ColumnRef),
    Cast {
        expr: Box<Expression>,
        ty: Type,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
    Like {
        str_expr: Box<Expression>,
        pattern: Box<Expression>,
        case_insensitive: bool,
    },
    Case {
        branches: Vec<CaseBranch>,
        else_branch: Option<Box<Expression>>,
    },
    Function(FunctionCall),
    ScalarSubquery(Box<Query>),
    Exists(Box<Query>),
    Parameter(NonZeroUsize),
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Constant(value) => std::fmt::Debug::fmt(value, f),
            Self::ColumnRef(column_ref) => column_ref.fmt(f),
            Self::Cast { expr, ty } => write!(f, "CAST({expr} AS {ty})"),
            Self::UnaryOp { op, expr } => {
                if op.is_prefix() {
                    write!(f, "({op} {expr})")
                } else {
                    write!(f, "({expr} {op})")
                }
            }
            Self::BinaryOp { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
            Self::Case {
                branches,
                else_branch,
            } => {
                f.write_str("CASE")?;
                for branch in branches {
                    write!(f, " {branch}")?;
                }
                if let Some(else_branch) = else_branch {
                    write!(f, " ELSE {else_branch}")?;
                }
                f.write_str(" END")
            }
            Self::Function(function_call) => function_call.fmt(f),
            Self::Like {
                str_expr,
                pattern,
                case_insensitive,
            } => {
                write!(f, "({str_expr} ")?;
                f.write_str(if *case_insensitive { "ILIKE" } else { "LIKE" })?;
                write!(f, " {pattern})")
            }
            Self::ScalarSubquery(query) => write!(f, "({query})"),
            Self::Exists(query) => write!(f, "EXISTS ({query})"),
            Self::Parameter(i) => write!(f, "${i}"),
        }
    }
}

impl Expression {
    fn unary_op(self, op: UnaryOp) -> Self {
        Self::UnaryOp {
            op,
            expr: self.into(),
        }
    }

    fn binary_op(self, op: BinaryOp, other: Self) -> Self {
        Self::BinaryOp {
            op,
            lhs: self.into(),
            rhs: other.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table_name: Option<String>,
    pub column_name: String,
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(table_name) = &self.table_name {
            write!(f, "{table_name}.")?;
        }
        f.write_str(&self.column_name)
    }
}

impl ColumnRef {
    pub fn qualified(table_name: String, column_name: String) -> Self {
        Self {
            table_name: Some(table_name),
            column_name,
        }
    }

    pub fn unqualified(column_name: String) -> Self {
        Self {
            table_name: None,
            column_name,
        }
    }
}

/*
Operator Precedence
https://www.postgresql.org/docs/16/sql-syntax-lexical.html#SQL-PRECEDENCE

|    | Operator/Element              | Associativity | Description                                        | Implemented |
|----|-------------------------------|---------------|----------------------------------------------------|------------ |
| 16 | .                             | left          | table/column name separator                        | Yes         |
| 15 | ::                            | left          | PostgreSQL-style typecast                          |             |
| 14 | [ ]                           | left          | array element selection                            |             |
| 13 | + -                           | right         | unary plus, unary minus                            | Yes         |
| 12 | COLLATE                       | left          | collation selection                                |             |
| 11 | AT                            | left          | AT TIME ZONE                                       |             |
| 10 | ^                             | left          | exponentiation                                     |             |
|  9 | * / %                         | left          | multiplication, division, modulo                   | Yes         |
|  8 | + -                           | left          | addition, subtraction                              | Yes         |
|  7 | (any other operator)          | left          | all other native and user-defined operators        | Partially   |
|  6 | BETWEEN IN LIKE ILIKE SIMILAR |               | range containment, set membership, string matching | Partially   |
|  5 | < > = <= >= <>                |               | comparison operators                               | Yes         |
|  4 | IS ISNULL NOTNULL             |               | IS TRUE, IS FALSE, IS NULL, IS DISTINCT FROM, etc. | Partially   |
|  3 | NOT                           | right         | logical negation                                   | Yes         |
|  2 | AND                           | left          | logical conjunction                                | Yes         |
|  1 | OR                            | left          | logical disjunction                                | Yes         |
 */

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
    IsNull,
    IsTrue,
    IsFalse,
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Not => "NOT",
            Self::IsNull => "IS NULL",
            Self::IsTrue => "IS TRUE",
            Self::IsFalse => "IS FALSE",
        })
    }
}

impl UnaryOp {
    pub fn is_prefix(self) -> bool {
        matches!(self, Self::Plus | Self::Minus | Self::Not)
    }

    const fn precedence(self) -> usize {
        match self {
            Self::Plus | Self::Minus => 13,
            Self::IsNull | Self::IsTrue | Self::IsFalse => 4,
            Self::Not => 3,
        }
    }
}

struct PostfixOp {
    op: UnaryOp,
    not: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Concat,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Eq => "=",
            Self::Ne => "<>",
            Self::Lt => "<",
            Self::Le => "<=",
            Self::Gt => ">",
            Self::Ge => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Concat => "||",
        })
    }
}

impl BinaryOp {
    const fn precedence(self) -> usize {
        match self {
            Self::Mul | Self::Div | Self::Mod => 9,
            Self::Add | Self::Sub => 8,
            Self::Concat => 7,
            Self::Eq | Self::Ne | Self::Lt | Self::Le | Self::Gt | Self::Ge => 5,
            Self::And => 2,
            Self::Or => 1,
        }
    }
}

enum InfixOp {
    Binary(BinaryOp),
    Like { case_insensitive: bool, not: bool },
}

impl InfixOp {
    const fn precedence(&self) -> usize {
        match self {
            Self::Binary(op) => op.precedence(),
            Self::Like { .. } => 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CaseBranch {
    pub condition: Expression,
    pub result: Expression,
}

impl std::fmt::Display for CaseBranch {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "WHEN {} THEN {}", self.condition, self.result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: String,
    pub args: FunctionArgs,
    pub is_distinct: bool,
}

impl std::fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}(", self.name)?;
        if self.is_distinct {
            f.write_str("DISTINCT ")?;
        }
        self.args.fmt(f)?;
        f.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FunctionArgs {
    Wildcard,
    Expressions(Vec<Expression>),
}

impl std::fmt::Display for FunctionArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Wildcard => f.write_str("*"),
            Self::Expressions(args) => {
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    arg.fmt(f)?;
                }
                Ok(())
            }
        }
    }
}

impl Parser<'_> {
    pub fn parse_expr(&mut self) -> ParserResult<Expression> {
        self.parse_sub_expr(0)
    }

    fn parse_sub_expr(&mut self, min_precedence: usize) -> ParserResult<Expression> {
        let mut expr = match self.try_parse_prefix_op()? {
            Some(op) => self.parse_sub_expr(op.precedence())?.unary_op(op),
            None => self.parse_atom()?,
        };
        loop {
            if let Some(PostfixOp { op, not }) = self.try_parse_postfix_op(min_precedence)? {
                expr = expr.unary_op(op);
                if not {
                    expr = expr.unary_op(UnaryOp::Not);
                }
            }
            let Some(op) = self.try_parse_infix_op(min_precedence)? else {
                break;
            };
            let rhs = self.parse_sub_expr(op.precedence())?;
            match op {
                InfixOp::Binary(op) => expr = expr.binary_op(op, rhs),
                InfixOp::Like {
                    case_insensitive,
                    not,
                } => {
                    expr = Expression::Like {
                        str_expr: Box::new(expr),
                        pattern: Box::new(rhs),
                        case_insensitive,
                    };
                    if not {
                        expr = expr.unary_op(UnaryOp::Not);
                    }
                }
            }
        }
        Ok(expr)
    }

    fn try_parse_prefix_op(&mut self) -> ParserResult<Option<UnaryOp>> {
        let op = match self.lexer.peek()? {
            Token::Plus => UnaryOp::Plus,
            Token::Minus => UnaryOp::Minus,
            Token::Not => UnaryOp::Not,
            _ => return Ok(None),
        };
        self.lexer.consume()?;
        Ok(Some(op))
    }

    fn try_parse_postfix_op(&mut self, min_precedence: usize) -> ParserResult<Option<PostfixOp>> {
        if *self.lexer.peek()? != Token::Is {
            return Ok(None);
        }
        let not = *self.lexer.lookahead(1)? == Token::Not;
        let token = if not {
            self.lexer.lookahead(2)? // Skip IS NOT
        } else {
            self.lexer.lookahead(1)? // Skip IS
        };
        let op = match token {
            Token::Null => UnaryOp::IsNull,
            Token::True => UnaryOp::IsTrue,
            Token::False => UnaryOp::IsFalse,
            _ if not => return Err(ParserError::unexpected(token)),
            _ => return Ok(None),
        };
        if op.precedence() <= min_precedence {
            return Ok(None);
        }
        self.expect(Token::Is)?;
        if not {
            self.expect(Token::Not)?;
        }
        self.lexer.consume()?;
        Ok(Some(PostfixOp { op, not }))
    }

    fn try_parse_infix_op(&mut self, min_precedence: usize) -> ParserResult<Option<InfixOp>> {
        let not = *self.lexer.peek()? == Token::Not;
        let token = if not {
            self.lexer.lookahead(1)? // Skip NOT
        } else {
            self.lexer.peek()?
        };
        let op = match token {
            Token::Like => InfixOp::Like {
                case_insensitive: false,
                not,
            },
            Token::ILike => InfixOp::Like {
                case_insensitive: true,
                not,
            },
            token if not => return Err(ParserError::unexpected(token)),
            Token::And => InfixOp::Binary(BinaryOp::And),
            Token::Or => InfixOp::Binary(BinaryOp::Or),
            Token::Percent => InfixOp::Binary(BinaryOp::Mod),
            Token::Asterisk => InfixOp::Binary(BinaryOp::Mul),
            Token::Plus => InfixOp::Binary(BinaryOp::Add),
            Token::Minus => InfixOp::Binary(BinaryOp::Sub),
            Token::Slash => InfixOp::Binary(BinaryOp::Div),
            Token::Lt => InfixOp::Binary(BinaryOp::Lt),
            Token::Eq => InfixOp::Binary(BinaryOp::Eq),
            Token::Gt => InfixOp::Binary(BinaryOp::Gt),
            Token::Ne => InfixOp::Binary(BinaryOp::Ne),
            Token::Le => InfixOp::Binary(BinaryOp::Le),
            Token::Ge => InfixOp::Binary(BinaryOp::Ge),
            Token::PipePipe => InfixOp::Binary(BinaryOp::Concat),
            _ => return Ok(None),
        };
        if op.precedence() <= min_precedence {
            return Ok(None);
        }
        if not {
            self.expect(Token::Not)?;
        }
        self.lexer.consume()?;
        Ok(Some(op))
    }

    fn parse_atom(&mut self) -> ParserResult<Expression> {
        let expr = match self.lexer.consume()? {
            Token::Null => Expression::Constant(Value::Null),
            Token::IntegerLiteral(i) => Expression::Constant(Value::Integer(i)),
            Token::RealLiteral(f) => Expression::Constant(Value::Real(f)),
            Token::True => Expression::Constant(Value::Boolean(true)),
            Token::False => Expression::Constant(Value::Boolean(false)),
            Token::String(s) => Expression::Constant(Value::Text(s)),
            Token::Identifier(ident) => match self.lexer.peek()? {
                Token::LeftParen => {
                    self.lexer.consume()?;
                    let is_distinct = self.lexer.consume_if_eq(Token::Distinct)?;
                    let args = if self.lexer.consume_if_eq(Token::RightParen)? {
                        FunctionArgs::Expressions(Vec::new())
                    } else if self.lexer.consume_if_eq(Token::Asterisk)? {
                        self.expect(Token::RightParen)?;
                        FunctionArgs::Wildcard
                    } else {
                        let exprs = self.parse_comma_separated(Self::parse_expr)?;
                        self.expect(Token::RightParen)?;
                        FunctionArgs::Expressions(exprs)
                    };
                    Expression::Function(FunctionCall {
                        name: ident,
                        args,
                        is_distinct,
                    })
                }
                Token::Dot => {
                    self.lexer.consume()?;
                    let column_name = self.expect_identifier()?;
                    Expression::ColumnRef(ColumnRef::qualified(ident, column_name))
                }
                _ => Expression::ColumnRef(ColumnRef::unqualified(ident)),
            },
            Token::Cast => {
                self.expect(Token::LeftParen)?;
                let expr = self.parse_expr()?;
                self.expect(Token::As)?;
                let ty = self.parse_type()?;
                self.expect(Token::RightParen)?;
                Expression::Cast {
                    expr: Box::new(expr),
                    ty,
                }
            }
            Token::Case => {
                let input_expr = (*self.lexer.peek()? != Token::When)
                    .then(|| self.parse_expr())
                    .transpose()?;
                let mut branches = Vec::new();
                while self.lexer.consume_if_eq(Token::When)? {
                    let mut condition = self.parse_expr()?;
                    if let Some(input_expr) = &input_expr {
                        condition = Expression::BinaryOp {
                            op: BinaryOp::Eq,
                            lhs: Box::new(input_expr.clone()),
                            rhs: Box::new(condition),
                        };
                    }
                    self.expect(Token::Then)?;
                    let result = self.parse_expr()?;
                    branches.push(CaseBranch { condition, result });
                }
                let else_branch = self
                    .lexer
                    .consume_if_eq(Token::Else)?
                    .then(|| self.parse_expr())
                    .transpose()?;
                self.expect(Token::End)?;
                Expression::Case {
                    branches,
                    else_branch: else_branch.map(Box::new),
                }
            }
            Token::Exists => {
                self.expect(Token::LeftParen)?;
                let query = self.parse_query()?;
                self.expect(Token::RightParen)?;
                Expression::Exists(Box::new(query))
            }
            Token::LeftParen => {
                let inner = match self.lexer.peek()? {
                    token if token.is_query_prefix() => {
                        let query = self.parse_query()?;
                        Expression::ScalarSubquery(Box::new(query))
                    }
                    _ => self.parse_expr()?,
                };
                self.expect(Token::RightParen)?;
                inner
            }
            Token::Parameter(i) => {
                self.max_param_id = self.max_param_id.max(Some(i));
                Expression::Parameter(i)
            }
            token => return Err(ParserError::unexpected(&token)),
        };
        Ok(expr)
    }
}
