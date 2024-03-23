use super::{Expression, Parser, ParserResult, Select};
use crate::lexer::Token;

#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub column_names: Option<Vec<String>>,
    pub select: Select,
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

impl Parser<'_> {
    pub fn parse_insert(&mut self) -> ParserResult<Insert> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table_name = self.expect_identifier()?;
        let column_names = self
            .lexer
            .consume_if_eq(Token::LeftParen)?
            .then(|| -> ParserResult<_> {
                let column_names = self.parse_comma_separated(Self::expect_identifier)?;
                self.expect(Token::RightParen)?;
                Ok(column_names)
            })
            .transpose()?;
        let select = self.parse_select()?;
        Ok(Insert {
            table_name,
            column_names,
            select,
        })
    }

    pub fn parse_update(&mut self) -> ParserResult<Update> {
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

    pub fn parse_delete(&mut self) -> ParserResult<Delete> {
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
}
