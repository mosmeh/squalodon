use std::str::Chars;

#[derive(Debug, thiserror::Error)]
pub enum LexerError {
    #[error("Unknown token {0:?}")]
    UnknownToken(String),

    #[error("Unexpected end of file")]
    UnexpectedEof,
}

type LexerResult<T> = std::result::Result<T, LexerError>;

macro_rules! keywords {
    ($($v:ident)*) => {
        #[derive(Clone, PartialEq)]
        pub enum Token {
            $($v,)*
            Percent,
            LeftParen,
            RightParen,
            Asterisk,
            Plus,
            Comma,
            Minus,
            Dot,
            Slash,
            Semicolon,
            Lt,
            Eq,
            Gt,
            Ne,
            Le,
            Ge,
            PipePipe,
            IntegerLiteral(i64),
            RealLiteral(f64),
            String(String),
            Identifier(String),
            Eof,
        }

        impl Token {
            fn parse_keyword(s: &str) -> Option<Self> {
                match () {
                    $(_ if s.eq_ignore_ascii_case(stringify!($v)) => Some(Self::$v),)*
                    _ => None,
                }
            }
        }

        impl std::fmt::Debug for Token {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(Self::$v => f.write_str(&stringify!($v).to_ascii_uppercase()),)*
                    Self::Percent => f.write_str("%"),
                    Self::LeftParen => f.write_str("("),
                    Self::RightParen => f.write_str(")"),
                    Self::Asterisk => f.write_str("*"),
                    Self::Plus => f.write_str("+"),
                    Self::Comma => f.write_str(","),
                    Self::Minus => f.write_str("-"),
                    Self::Dot => f.write_str("."),
                    Self::Slash => f.write_str("/"),
                    Self::Semicolon => f.write_str(";"),
                    Self::Lt => f.write_str("<"),
                    Self::Eq => f.write_str("="),
                    Self::Gt => f.write_str(">"),
                    Self::Ne => f.write_str("<>"),
                    Self::Le => f.write_str("<="),
                    Self::Ge => f.write_str(">="),
                    Self::PipePipe => f.write_str("||"),
                    Self::IntegerLiteral(i) => i.fmt(f),
                    Self::RealLiteral(r) => r.fmt(f),
                    Self::String(s) => s.fmt(f),
                    Self::Identifier(i) => i.fmt(f),
                    Self::Eof => f.write_str("EOF"),
                }
            }
        }
    }
}

keywords! {
    And
    As
    Asc
    Begin
    Boolean
    By
    Commit
    Create
    Cross
    Delete
    Desc
    Describe
    Drop
    Exists
    Explain
    False
    First
    From
    If
    Inner
    Insert
    Integer
    Into
    Join
    Key
    Last
    Limit
    Not
    Null
    Nulls
    Offset
    On
    Or
    Order
    Primary
    Real
    Rollback
    Select
    Set
    Show
    Table
    Tables
    Text
    Transaction
    True
    Update
    Values
    Where
}

pub struct Lexer<'a> {
    inner: LexerInner<'a>,
    peeked: Option<Token>,
}

impl<'a> Lexer<'a> {
    pub fn new(s: &'a str) -> Self {
        Self {
            inner: LexerInner::new(s),
            peeked: None,
        }
    }

    pub fn consume(&mut self) -> LexerResult<Token> {
        if let Some(peeked) = self.peeked.take() {
            Ok(peeked)
        } else {
            self.inner.consume_token()
        }
    }

    pub fn consume_if<F>(&mut self, f: F) -> LexerResult<Option<Token>>
    where
        F: Fn(&Token) -> bool,
    {
        Ok(if f(self.peek()?) {
            Some(self.consume()?)
        } else {
            None
        })
    }

    pub fn consume_if_eq(&mut self, token: Token) -> LexerResult<bool> {
        Ok(self.consume_if(|t| *t == token)?.is_some())
    }

    pub fn peek(&mut self) -> LexerResult<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.consume()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }
}

struct LexerInner<'a> {
    chars: Chars<'a>,
    peeked: Option<char>,
}

impl<'a> LexerInner<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            chars: s.chars(),
            peeked: None,
        }
    }

    fn consume_token(&mut self) -> LexerResult<Token> {
        while let Some(ch) = self.peek() {
            match ch {
                _ if ch.is_control() | ch.is_whitespace() => {
                    self.consume().unwrap();
                }
                _ if ch.is_ascii_digit() => {
                    let literal = self.consume_while(|ch| ch.is_ascii_digit() || ch == '.');
                    return literal
                        .parse()
                        .map(Token::IntegerLiteral)
                        .or_else(|_| literal.parse().map(Token::RealLiteral))
                        .map_err(|_| LexerError::UnknownToken(literal));
                }
                '\'' => {
                    let s = self.consume_string('\'')?;
                    return Ok(Token::String(s));
                }
                '"' => {
                    let ident = self.consume_string('"')?;
                    return Ok(Token::Identifier(ident));
                }
                _ if ch.is_alphabetic() => {
                    let mut s =
                        self.consume_while(|ch| ch.is_alphanumeric() || matches!(ch, '$' | '_'));
                    return Ok(Token::parse_keyword(&s).unwrap_or_else(|| {
                        s.make_ascii_lowercase();
                        Token::Identifier(s)
                    }));
                }
                _ => {
                    return Ok(if self.consume_if_eq('%') {
                        Token::Percent
                    } else if self.consume_if_eq('(') {
                        Token::LeftParen
                    } else if self.consume_if_eq(')') {
                        Token::RightParen
                    } else if self.consume_if_eq('*') {
                        Token::Asterisk
                    } else if self.consume_if_eq('+') {
                        Token::Plus
                    } else if self.consume_if_eq(',') {
                        Token::Comma
                    } else if self.consume_if_eq('-') {
                        if self.consume_if_eq('-') {
                            // Comment
                            self.consume_while(|ch| ch != '\n');
                            continue;
                        }
                        Token::Minus
                    } else if self.consume_if_eq('.') {
                        Token::Dot
                    } else if self.consume_if_eq('/') {
                        Token::Slash
                    } else if self.consume_if_eq(';') {
                        Token::Semicolon
                    } else if self.consume_if_eq('<') {
                        if self.consume_if_eq('>') {
                            Token::Ne
                        } else if self.consume_if_eq('=') {
                            Token::Le
                        } else {
                            Token::Lt
                        }
                    } else if self.consume_if_eq('=') {
                        Token::Eq
                    } else if self.consume_if_eq('>') {
                        if self.consume_if_eq('=') {
                            Token::Ge
                        } else {
                            Token::Gt
                        }
                    } else if self.consume_if_eq('!') {
                        if self.consume_if_eq('=') {
                            Token::Ne
                        } else {
                            return Err(LexerError::UnknownToken("!".to_owned()));
                        }
                    } else if self.consume_if_eq('|') {
                        if self.consume_if_eq('|') {
                            Token::PipePipe
                        } else {
                            return Err(LexerError::UnknownToken("|".to_owned()));
                        }
                    } else {
                        return Err(LexerError::UnknownToken(ch.to_string()));
                    });
                }
            }
        }
        Ok(Token::Eof)
    }

    fn consume_string(&mut self, quote: char) -> LexerResult<String> {
        let ch = self.consume().unwrap();
        assert_eq!(ch, quote);
        let mut s = String::new();
        while let Some(ch) = self.consume() {
            if ch != quote {
                s.push(ch);
                continue;
            }
            if self.consume_if_eq(quote) {
                s.push(quote);
            } else {
                return Ok(s);
            }
        }
        Err(LexerError::UnexpectedEof)
    }

    fn consume(&mut self) -> Option<char> {
        self.peeked.take().or_else(|| self.chars.next())
    }

    fn consume_if<F>(&mut self, f: F) -> Option<char>
    where
        F: Fn(char) -> bool,
    {
        if let Some(ch) = self.peek() {
            if f(ch) {
                return self.consume();
            }
        }
        None
    }

    fn consume_if_eq(&mut self, ch: char) -> bool {
        self.consume_if(|c| c == ch).is_some()
    }

    fn consume_while<F>(&mut self, f: F) -> String
    where
        F: Fn(char) -> bool,
    {
        let mut s = String::new();
        while let Some(ch) = self.consume_if(&f) {
            s.push(ch);
        }
        s
    }

    fn peek(&mut self) -> Option<char> {
        if self.peeked.is_none() {
            self.peeked = self.chars.next();
        }
        self.peeked
    }
}

pub fn quote(s: &str, quote: char) -> String {
    let mut quoted = String::new();
    quoted.push(quote);
    for ch in s.chars() {
        if ch == quote {
            quoted.push(quote);
        }
        quoted.push(ch);
    }
    quoted.push(quote);
    quoted
}
