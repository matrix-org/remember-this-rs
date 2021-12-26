use std::fmt;
/// An error when dealing with the database.
#[derive(Debug, Clone)]
pub enum Error<E> {
    /// An error in the database
    Database(sled::Error),

    /// An error in the function we're caching
    Client(E),
}

impl<T: fmt::Display + fmt::Debug> fmt::Display for Error<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Database(e) => write!(f, "{}", e),
            Error::Client(e) => write!(f, "{}", e),
        }
    }
}
impl<T: fmt::Display + fmt::Debug> std::error::Error for Error<T> {}
