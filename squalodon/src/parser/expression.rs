use super::{unexpected, Parser, ParserResult, Query};
use crate::{lexer::Token, types::Value, Type};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(value) => value.fmt(f),
            Self::ColumnRef(column_ref) => column_ref.fmt(f),
            Self::Cast { expr, ty } => write!(f, "CAST({expr} AS {ty})"),
            Self::UnaryOp { op, expr } => write!(f, "({op} {expr})"),
            Self::BinaryOp { op, lhs, rhs } => write!(f, "({lhs} {op} {rhs})"),
            Self::Case {
                branches,
                else_branch,
            } => {
                write!(f, "CASE")?;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    pub table_name: Option<String>,
    pub column_name: String,
}

impl std::fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(table_name) = &self.table_name {
            write!(f, "{table_name}.")?;
        }
        f.write_str(&self.column_name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

impl std::fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Not => "NOT",
        })
    }
}

impl UnaryOp {
    fn from_token(token: &Token) -> Option<Self> {
        Some(match token {
            Token::Plus => Self::Plus,
            Token::Minus => Self::Minus,
            Token::Not => Self::Not,
            _ => return None,
        })
    }

    fn priority(self) -> usize {
        // https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
        match self {
            Self::Plus | Self::Minus => 9,
            Self::Not => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    fn from_token(token: &Token) -> Option<Self> {
        Some(match token {
            Token::And => Self::And,
            Token::Or => Self::Or,
            Token::Percent => Self::Mod,
            Token::Asterisk => Self::Mul,
            Token::Plus => Self::Add,
            Token::Minus => Self::Sub,
            Token::Slash => Self::Div,
            Token::Lt => Self::Lt,
            Token::Eq => Self::Eq,
            Token::Gt => Self::Gt,
            Token::Ne => Self::Ne,
            Token::Le => Self::Le,
            Token::Ge => Self::Ge,
            Token::PipePipe => Self::Concat,
            _ => return None,
        })
    }

    const fn priority(self) -> usize {
        // https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
        match self {
            Self::Concat => 8,
            Self::Mul | Self::Div | Self::Mod => 7,
            Self::Add | Self::Sub => 6,
            Self::Lt | Self::Le | Self::Gt | Self::Ge => 5,
            Self::Eq | Self::Ne => 4,
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
    fn priority(&self) -> usize {
        // https://www.sqlite.org/lang_expr.html#operators_and_parse_affecting_attributes
        match self {
            Self::Binary(op) => op.priority(),
            Self::Like { .. } => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CaseBranch {
    pub condition: Expression,
    pub result: Expression,
}

impl std::fmt::Display for CaseBranch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WHEN {} THEN {}", self.condition, self.result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionCall {
    pub name: String,
    pub args: FunctionArgs,
    pub is_distinct: bool,
}

impl std::fmt::Display for FunctionCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}(", self.name)?;
        if self.is_distinct {
            f.write_str("DISTINCT ")?;
        }
        self.args.fmt(f)?;
        f.write_str(")")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionArgs {
    Wildcard,
    Expressions(Vec<Expression>),
}

impl std::fmt::Display for FunctionArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

    fn parse_sub_expr(&mut self, min_priority: usize) -> ParserResult<Expression> {
        let mut expr = match UnaryOp::from_token(self.lexer.peek()?) {
            Some(op) => {
                self.lexer.consume()?;
                let expr = self.parse_sub_expr(op.priority())?;
                Expression::UnaryOp {
                    op,
                    expr: expr.into(),
                }
            }
            None => self.parse_atom()?,
        };
        loop {
            let Some(op) = self.try_parse_infix_op(min_priority)? else {
                break;
            };
            let rhs = self.parse_sub_expr(op.priority())?;
            match op {
                InfixOp::Binary(op) => {
                    expr = Expression::BinaryOp {
                        op,
                        lhs: expr.into(),
                        rhs: rhs.into(),
                    };
                }
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
                        expr = Expression::UnaryOp {
                            op: UnaryOp::Not,
                            expr: expr.into(),
                        };
                    }
                }
            }
        }
        Ok(expr)
    }

    fn try_parse_infix_op(&mut self, min_priority: usize) -> ParserResult<Option<InfixOp>> {
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
            token if not => return Err(unexpected(token)),
            token => match BinaryOp::from_token(token) {
                Some(op) => InfixOp::Binary(op),
                None => return Ok(None),
            },
        };
        if op.priority() <= min_priority {
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
                    let column = self.expect_identifier()?;
                    Expression::ColumnRef(ColumnRef {
                        table_name: Some(ident),
                        column_name: column,
                    })
                }
                _ => Expression::ColumnRef(ColumnRef {
                    table_name: None,
                    column_name: ident,
                }),
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
                    Token::Select | Token::Values | Token::LeftParen => {
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
            token => return Err(unexpected(&token)),
        };
        Ok(expr)
    }
}
