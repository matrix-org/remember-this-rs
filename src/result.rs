/// An error when dealing with the database.
#[derive(Debug, Clone)]
pub enum Error<E> {
    /// An error in the database
    Database(sled::Error),

    /// An error in the function we're caching
    Client(E),
}
