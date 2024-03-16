mod lexer;

pub use lexer::LexerError;

use crate::{
    catalog::Column,
    types::{Type, Value},
    BinaryOp, NullOrder, Order, UnaryOp,
};
use lexer::{Lexer, Token};

#[derive(thiserror::Error, Debug)]
pub enum ParserError {
    #[error("Unexpected token {0:?}")]
    UnexpectedToken(Token),

    #[error("Rows in VALUES must have the same number of columns")]
    ValuesColumnCountMismatch,

    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
}

pub type ParserResult<T> = std::result::Result<T, ParserError>;

#[derive(Debug)]
pub enum Statement {
    Explain(Box<Statement>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Insert(Insert),
    Values(Values),
    Select(Select),
    Update(Update),
    Delete(Delete),
    Transaction(TransactionControl),
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Column>,
}

#[derive(Debug)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub values: Values,
}

#[derive(Debug)]
pub struct Values {
    pub rows: Vec<Vec<Expression>>,
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
    Function { name: String, args: Vec<Expression> },
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

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(Value),
    ColumnRef(String),
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
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

    fn priority(self) -> usize {
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

#[derive(Debug, Clone, Copy)]
pub enum TransactionControl {
    Begin,
    Commit,
    Rollback,
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
}

impl Iterator for Parser<'_> {
    type Item = ParserResult<Statement>;

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

    fn parse_statement(&mut self) -> ParserResult<Statement> {
        match self.lexer.peek()? {
            Token::Explain => {
                self.lexer.consume()?;
                Ok(Statement::Explain(Box::new(self.parse_statement_inner()?)))
            }
            Token::Begin => {
                self.lexer.consume()?;
                self.lexer.consume_if_eq(Token::Transaction)?;
                Ok(Statement::Transaction(TransactionControl::Begin))
            }
            Token::Commit => {
                self.lexer.consume()?;
                self.lexer.consume_if_eq(Token::Transaction)?;
                Ok(Statement::Transaction(TransactionControl::Commit))
            }
            Token::Rollback => {
                self.lexer.consume()?;
                self.lexer.consume_if_eq(Token::Transaction)?;
                Ok(Statement::Transaction(TransactionControl::Rollback))
            }
            _ => self.parse_statement_inner(),
        }
    }

    fn parse_statement_inner(&mut self) -> ParserResult<Statement> {
        match self.lexer.peek()? {
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Values => self.parse_values().map(Statement::Values),
            Token::Select => self.parse_select().map(Statement::Select),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            token => Err(ParserError::UnexpectedToken(token.clone())),
        }
    }

    fn parse_create(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Create)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            token => Err(ParserError::UnexpectedToken(token.clone())),
        }
    }

    fn parse_create_table(&mut self) -> ParserResult<CreateTable> {
        self.expect(Token::Table)?;
        let if_not_exists = self
            .lexer
            .consume_if_eq(Token::If)?
            .then(|| {
                self.expect(Token::Not)?;
                self.expect(Token::Exists)
            })
            .transpose()?
            .is_some();
        let name = self.expect_identifier()?;
        self.expect(Token::LeftParen)?;
        let columns = self.parse_comma_separated(Self::parse_column_definition)?;
        self.expect(Token::RightParen)?;
        Ok(CreateTable {
            name,
            if_not_exists,
            columns,
        })
    }

    fn parse_drop(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Drop)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_drop_table().map(Statement::DropTable),
            token => Err(ParserError::UnexpectedToken(token.clone())),
        }
    }

    fn parse_drop_table(&mut self) -> ParserResult<DropTable> {
        self.expect(Token::Table)?;
        let if_exists = self
            .lexer
            .consume_if_eq(Token::If)?
            .then(|| self.expect(Token::Exists))
            .transpose()?
            .is_some();
        let name = self.expect_identifier()?;
        Ok(DropTable { name, if_exists })
    }

    fn parse_column_definition(&mut self) -> ParserResult<Column> {
        let name = self.expect_identifier()?;
        let ty = match self.lexer.consume()? {
            Token::Integer => Type::Integer,
            Token::Real => Type::Real,
            Token::Boolean => Type::Boolean,
            Token::Text => Type::Text,
            token => return Err(ParserError::UnexpectedToken(token)),
        };
        let mut is_primary_key = false;
        let mut is_nullable = true;
        loop {
            match self.lexer.peek()? {
                Token::Primary => {
                    self.lexer.consume()?;
                    self.expect(Token::Key)?;
                    is_primary_key = true;
                    is_nullable = false;
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

    fn parse_insert(&mut self) -> ParserResult<Insert> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table_name = self.expect_identifier()?;
        let mut column_names = Vec::new();
        if self.lexer.consume_if_eq(Token::LeftParen)? {
            column_names = self.parse_comma_separated(Self::expect_identifier)?;
            self.expect(Token::RightParen)?;
        }
        let values = self.parse_values()?;
        Ok(Insert {
            table_name,
            column_names,
            values,
        })
    }

    fn parse_values(&mut self) -> ParserResult<Values> {
        self.expect(Token::Values)?;
        let mut num_columns = None;
        let rows = self.parse_comma_separated(|parser| {
            parser.expect(Token::LeftParen)?;
            let exprs = parser.parse_comma_separated(Self::parse_expr)?;
            parser.expect(Token::RightParen)?;
            match num_columns {
                None => num_columns = Some(exprs.len()),
                Some(n) if n != exprs.len() => return Err(ParserError::ValuesColumnCountMismatch),
                _ => (),
            }
            Ok(exprs)
        })?;
        Ok(Values { rows })
    }

    fn parse_select(&mut self) -> ParserResult<Select> {
        self.expect(Token::Select)?;
        let projections = self.parse_comma_separated(Self::parse_projection)?;
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

    fn parse_projection(&mut self) -> ParserResult<Projection> {
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

    fn parse_table_ref(&mut self) -> ParserResult<TableRef> {
        let name = self.expect_identifier()?;
        if *self.lexer.peek()? == Token::LeftParen {
            let args = self.parse_args()?;
            Ok(TableRef::Function { name, args })
        } else {
            Ok(TableRef::BaseTable { name })
        }
    }

    fn parse_order_by(&mut self) -> ParserResult<Vec<OrderBy>> {
        self.expect(Token::Order)?;
        self.expect(Token::By)?;
        self.parse_comma_separated(|parser| {
            let expr = parser.parse_expr()?;

            let order = if parser.lexer.consume_if_eq(Token::Asc)? {
                Order::Asc
            } else if parser.lexer.consume_if_eq(Token::Desc)? {
                Order::Desc
            } else {
                Default::default()
            };

            let null_order = if parser.lexer.consume_if_eq(Token::Nulls)? {
                match parser.lexer.consume()? {
                    Token::First => NullOrder::NullsFirst,
                    Token::Last => NullOrder::NullsLast,
                    token => return Err(ParserError::UnexpectedToken(token)),
                }
            } else {
                Default::default()
            };

            Ok(OrderBy {
                expr,
                order,
                null_order,
            })
        })
    }

    fn parse_update(&mut self) -> ParserResult<Update> {
        self.expect(Token::Update)?;
        let table_name = self.expect_identifier()?;
        self.expect(Token::Set)?;
        let sets = self.parse_comma_separated(Self::parse_set)?;
        let where_clause = self
            .lexer
            .consume_if_eq(Token::Where)?
            .then(|| self.parse_expr())
            .transpose()?;
        Ok(Update {
            table_name,
            sets,
            where_clause,
        })
    }

    fn parse_set(&mut self) -> ParserResult<Set> {
        let column_name = self.expect_identifier()?;
        self.expect(Token::Eq)?;
        let expr = self.parse_expr()?;
        Ok(Set { column_name, expr })
    }

    fn parse_delete(&mut self) -> ParserResult<Delete> {
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

    fn parse_expr(&mut self) -> ParserResult<Expression> {
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

    fn parse_atom(&mut self) -> ParserResult<Expression> {
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
            token => return Err(ParserError::UnexpectedToken(token)),
        };
        Ok(expr)
    }

    fn parse_comma_separated<T, F>(&mut self, mut f: F) -> ParserResult<Vec<T>>
    where
        F: FnMut(&mut Self) -> ParserResult<T>,
    {
        let mut items = Vec::new();
        loop {
            items.push(f(self)?);
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        Ok(items)
    }

    fn parse_args(&mut self) -> ParserResult<Vec<Expression>> {
        self.expect(Token::LeftParen)?;
        if self.lexer.consume_if_eq(Token::RightParen)? {
            return Ok(Vec::new());
        }
        let args = self.parse_comma_separated(Self::parse_expr)?;
        self.expect(Token::RightParen)?;
        Ok(args)
    }

    fn expect(&mut self, expected: Token) -> ParserResult<()> {
        self.expect_one_of(&[expected])
    }

    fn expect_one_of(&mut self, expected: &[Token]) -> ParserResult<()> {
        let actual = self.lexer.consume()?;
        if !expected.contains(&actual) {
            return Err(ParserError::UnexpectedToken(actual));
        }
        Ok(())
    }

    fn expect_identifier(&mut self) -> ParserResult<String> {
        match self.lexer.consume()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(ParserError::UnexpectedToken(token)),
        }
    }
}
