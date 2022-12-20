use serde::{de::DeserializeOwned, Serialize};

use super::Message;

impl<T> Message for T where T: 
    Clone + 
    std::fmt::Debug + 
    Serialize + 
    DeserializeOwned +
    Send +
    Sync +
    'static
{}