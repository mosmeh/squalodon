use super::{unexpected, Parser, ParserResult, Statement};
use crate::{catalog::Column, lexer::Token};

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Constraint {
    PrimaryKey(Vec<String>),
    NotNull(String),
}

#[derive(Debug, Clone)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

impl Parser<'_> {
    pub fn parse_create(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Create)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            token => Err(unexpected(token)),
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
        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        loop {
            match self.lexer.peek()? {
                Token::Primary => {
                    self.lexer.consume()?;
                    self.expect(Token::Key)?;
                    self.expect(Token::LeftParen)?;
                    constraints.push(Constraint::PrimaryKey(
                        self.parse_comma_separated(Self::expect_identifier)?,
                    ));
                    self.expect(Token::RightParen)?;
                }
                Token::Identifier(_) => {
                    let name = self.expect_identifier()?;
                    let ty = self.parse_type()?;
                    loop {
                        match self.lexer.peek()? {
                            Token::Primary => {
                                self.lexer.consume()?;
                                self.expect(Token::Key)?;
                                constraints.push(Constraint::PrimaryKey(vec![name.clone()]));
                            }
                            Token::Not => {
                                self.lexer.consume()?;
                                self.expect(Token::Null)?;
                                constraints.push(Constraint::NotNull(name.clone()));
                            }
                            _ => break,
                        }
                    }
                    columns.push(Column { name, ty });
                }
                token => return Err(unexpected(token)),
            }
            if !self.lexer.consume_if_eq(Token::Comma)? {
                break;
            }
        }
        self.expect(Token::RightParen)?;
        Ok(CreateTable {
            name,
            if_not_exists,
            columns,
            constraints,
        })
    }

    pub fn parse_drop(&mut self) -> ParserResult<Statement> {
        self.expect(Token::Drop)?;
        match self.lexer.peek()? {
            Token::Table => self.parse_drop_table().map(Statement::DropTable),
            token => Err(unexpected(token)),
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
}
