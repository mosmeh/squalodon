mod lexer;

pub use lexer::Error as LexerError;
pub use Error as ParserError;

use crate::{
    storage::Column,
    types::{Type, Value},
    BinaryOp, NullOrder, Order,
};
use lexer::{Lexer, Token};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unexpected token {0:?}")]
    UnexpectedToken(Token),

    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Statement {
    Explain(Box<Statement>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Insert(Insert),
    Select(Select),
    Update(Update),
    Delete(Delete),
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct DropTable {
    pub name: String,
}

#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub exprs: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(Value),
    ColumnRef(String),
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
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
            _ => return None,
        })
    }

    fn priority(self) -> usize {
        match self {
            Self::Mul | Self::Div | Self::Mod => 6,
            Self::Add | Self::Sub => 5,
            Self::Lt | Self::Le | Self::Gt | Self::Ge => 4,
            Self::Eq | Self::Ne => 3,
            Self::And => 2,
            Self::Or => 1,
        }
    }
}

#[derive(Debug)]
pub struct Select {
    pub projections: Vec<Projection>,
    pub from: Option<TableRef>,
    pub where_clause: Option<Expression>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

#[derive(Debug)]
pub enum Projection {
    Wildcard,
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

#[derive(Debug)]
pub enum TableRef {
    BaseTable { name: String },
}

#[derive(Debug)]
pub struct OrderBy {
    pub expr: Expression,
    pub order: Order,
    pub null_order: NullOrder,
}

#[derive(Debug)]
pub struct Update {
    pub table_name: String,
    pub sets: Vec<Set>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug)]
pub struct Set {
    pub column_name: String,
    pub expr: Expression,
}

#[derive(Debug)]
pub struct Delete {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl Iterator for Parser<'_> {
    type Item = Result<Statement>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.lexer.peek() {
                Ok(Token::Eof) => return None,
                Ok(Token::Semicolon) => {
                    self.lexer.consume().unwrap();
                }
                Ok(_) => break,
                Err(e) => return Some(Err(e.into())),
            }
        }
        let statement = match self.parse_statement() {
            Ok(statement) => statement,
            Err(e) => return Some(Err(e)),
        };
        Some(match self.expect_one_of(&[Token::Semicolon, Token::Eof]) {
            Ok(()) => Ok(statement),
            Err(e) => Err(e),
        })
    }
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str) -> Self {
        Self {
            lexer: Lexer::new(s),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        if self.lexer.consume_if_eq(Token::Explain)? {
            return Ok(Statement::Explain(Box::new(self.parse_statement_inner()?)));
        }
        self.parse_statement_inner()
    }

    fn parse_statement_inner(&mut self) -> Result<Statement> {
        match self.lexer.peek()? {
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Select => self.parse_select().map(Statement::Select),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            token => Err(Error::UnexpectedToken(token.clone())),
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(Token::Create)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            token => Err(Error::UnexpectedToken(token.clone())),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTable> {
        self.expect(Token::Table)?;
        let name = self.expect_identifier()?;
        self.expect(Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_column_definition()?);
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        self.expect(Token::RightParen)?;
        Ok(CreateTable { name, columns })
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(Token::Drop)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_drop_table().map(Statement::DropTable),
            token => Err(Error::UnexpectedToken(token.clone())),
        }
    }

    fn parse_drop_table(&mut self) -> Result<DropTable> {
        self.expect(Token::Table)?;
        let name = self.expect_identifier()?;
        Ok(DropTable { name })
    }

    fn parse_column_definition(&mut self) -> Result<Column> {
        let name = self.expect_identifier()?;
        let ty = match self.lexer.consume()? {
            Token::Integer => Type::Integer,
            Token::Real => Type::Real,
            Token::Boolean => Type::Boolean,
            Token::Text => Type::Text,
            token => return Err(Error::UnexpectedToken(token)),
        };
        let mut is_primary_key = false;
        let mut is_nullable = true;
        loop {
            match self.lexer.peek()? {
                Token::Primary => {
                    self.lexer.consume()?;
                    self.expect(Token::Key)?;
                    is_primary_key = true;
                }
                Token::Not => {
                    self.lexer.consume()?;
                    self.expect(Token::Null)?;
                    is_nullable = false;
                }
                _ => break,
            }
        }
        Ok(Column {
            name,
            ty,
            is_primary_key,
            is_nullable,
        })
    }

    fn parse_insert(&mut self) -> Result<Insert> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table_name = self.expect_identifier()?;
        let mut column_names = Vec::new();
        if self.lexer.consume_if_eq(Token::LeftParen)? {
            loop {
                column_names.push(self.expect_identifier()?);
                if !self.lexer.consume_if_eq(Token::Comma)? {
                    break;
                }
            }
            self.expect(Token::RightParen)?;
        }
        self.expect(Token::Values)?;
        self.expect(Token::LeftParen)?;
        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_expr()?);
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        self.expect(Token::RightParen)?;
        Ok(Insert {
            table_name,
            column_names,
            exprs,
        })
    }

    fn parse_select(&mut self) -> Result<Select> {
        self.expect(Token::Select)?;
        let mut projections = Vec::new();
        loop {
            projections.push(self.parse_projection()?);
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        let from = self
            .lexer
            .consume_if_eq(Token::From)?
            .then(|| self.parse_table_ref())
            .transpose()?;
        let where_clause = self
            .lexer
            .consume_if_eq(Token::Where)?
            .then(|| self.parse_expr())
            .transpose()?;
        let order_by = (*self.lexer.peek()? == Token::Order)
            .then(|| self.parse_order_by())
            .transpose()?
            .unwrap_or_default();
        let limit = self
            .lexer
            .consume_if_eq(Token::Limit)?
            .then(|| self.parse_expr())
            .transpose()?;
        let offset = self
            .lexer
            .consume_if_eq(Token::Offset)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Select {
            projections,
            from,
            where_clause,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_projection(&mut self) -> Result<Projection> {
        if self.lexer.consume_if_eq(Token::Asterisk)? {
            Ok(Projection::Wildcard)
        } else {
            let expr = self.parse_expr()?;
            let alias = self
                .lexer
                .consume_if_eq(Token::As)?
                .then(|| self.expect_identifier())
                .transpose()?;
            Ok(Projection::Expression { expr, alias })
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = self.expect_identifier()?;
        Ok(TableRef::BaseTable { name })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderBy>> {
        self.expect(Token::Order)?;
        self.expect(Token::By)?;
        let mut order_by = Vec::new();
        loop {
            let expr = self.parse_expr()?;

            let order = if self.lexer.consume_if_eq(Token::Asc)? {
                Order::Asc
            } else if self.lexer.consume_if_eq(Token::Desc)? {
                Order::Desc
            } else {
                Default::default()
            };

            let null_order = if self.lexer.consume_if_eq(Token::Nulls)? {
                match self.lexer.consume()? {
                    Token::First => NullOrder::NullsFirst,
                    Token::Last => NullOrder::NullsLast,
                    token => return Err(Error::UnexpectedToken(token)),
                }
            } else {
                Default::default()
            };

            order_by.push(OrderBy {
                expr,
                order,
                null_order,
            });
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        Ok(order_by)
    }

    fn parse_update(&mut self) -> Result<Update> {
        self.expect(Token::Update)?;
        let table_name = self.expect_identifier()?;
        self.expect(Token::Set)?;
        let mut set = Vec::new();
        loop {
            set.push(self.parse_set()?);
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        let where_clause = self
            .lexer
            .consume_if_eq(Token::Where)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Update {
            table_name,
            sets: set,
            where_clause,
        })
    }

    fn parse_set(&mut self) -> Result<Set> {
        let column_name = self.expect_identifier()?;
        self.expect(Token::Eq)?;
        let expr = self.parse_expr()?;
        Ok(Set { column_name, expr })
    }

    fn parse_delete(&mut self) -> Result<Delete> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        let table_name = self.expect_identifier()?;
        let where_clause = self
            .lexer
            .consume_if_eq(Token::Where)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Delete {
            table_name,
            where_clause,
        })
    }

    fn parse_expr(&mut self) -> Result<Expression> {
        self.parse_sub_expr(0)
    }

    fn parse_sub_expr(&mut self, min_priority: usize) -> Result<Expression> {
        let mut expr = self.parse_atom()?;
        loop {
            let Some(op) = BinaryOp::from_token(self.lexer.peek()?) else {
                break;
            };
            let priority = op.priority();
            if priority <= min_priority {
                break;
            }
            self.lexer.consume()?;
            let rhs = self.parse_sub_expr(priority)?;
            expr = Expression::BinaryOp {
                op,
                lhs: expr.into(),
                rhs: rhs.into(),
            };
        }
        Ok(expr)
    }

    fn parse_atom(&mut self) -> Result<Expression> {
        let expr = match self.lexer.consume()? {
            Token::Null => Expression::Constant(Value::Null),
            Token::IntegerLiteral(i) => Expression::Constant(Value::Integer(i)),
            Token::RealLiteral(f) => Expression::Constant(Value::Real(f)),
            Token::True => Expression::Constant(Value::Boolean(true)),
            Token::False => Expression::Constant(Value::Boolean(false)),
            Token::String(s) => Expression::Constant(Value::Text(s)),
            Token::Identifier(ident) => Expression::ColumnRef(ident),
            Token::LeftParen => {
                let expr = self.parse_expr()?;
                self.expect(Token::RightParen)?;
                expr
            }
            token => return Err(Error::UnexpectedToken(token)),
        };
        Ok(expr)
    }

    fn expect(&mut self, expected: Token) -> Result<()> {
        self.expect_one_of(&[expected])
    }

    fn expect_one_of(&mut self, expected: &[Token]) -> Result<()> {
        let actual = self.lexer.consume()?;
        if !expected.contains(&actual) {
            return Err(Error::UnexpectedToken(actual));
        }
        Ok(())
    }

    fn expect_identifier(&mut self) -> Result<String> {
        match self.lexer.consume()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(Error::UnexpectedToken(token)),
        }
    }
}
