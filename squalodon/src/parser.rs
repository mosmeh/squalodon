mod ddl;
mod expression;
mod modification;
mod query;

pub use ddl::{Constraint, CreateTable, DropTable};
pub use expression::{BinaryOp, ColumnRef, Expression, UnaryOp};
pub use modification::{Delete, Insert, Update};
pub use query::{Join, NullOrder, Order, OrderBy, Projection, Select, TableRef, Values};

use crate::lexer::{Lexer, LexerError, Token};

#[derive(thiserror::Error, Debug)]
pub enum ParserError {
    #[error("Unexpected token {0}")]
    UnexpectedToken(String),

    #[error("Rows in VALUES must have the same number of columns")]
    ValuesColumnCountMismatch,

    #[error("Lexer error: {0}")]
    Lexer(#[from] LexerError),
}

pub type ParserResult<T> = std::result::Result<T, ParserError>;

#[derive(Debug)]
pub enum Statement {
    Explain(Box<Statement>),
    Transaction(TransactionControl),
    ShowTables,
    Describe(String),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
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
            Token::Explain => {
                self.lexer.consume()?;
                Ok(Statement::Explain(Box::new(self.parse_statement_inner()?)))
            }
            Token::Show => {
                self.lexer.consume()?;
                self.expect(Token::Tables)?;
                Ok(Statement::ShowTables)
            }
            Token::Describe => {
                self.lexer.consume()?;
                let name = self.expect_identifier()?;
                Ok(Statement::Describe(name))
            }
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Select | Token::Values => self.parse_select().map(Statement::Select),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            token => Err(unexpected(token)),
        }
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
            return Err(unexpected(&actual));
        }
        Ok(())
    }

    fn expect_identifier(&mut self) -> ParserResult<String> {
        match self.lexer.consume()? {
            Token::Identifier(ident) => Ok(ident),
            token => Err(unexpected(&token)),
        }
    }
}

fn unexpected(token: &Token) -> ParserError {
    ParserError::UnexpectedToken(format!("{token:?}"))
}
