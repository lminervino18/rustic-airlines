/// Returns true if the token is equal to "AND".
pub fn is_and(token: &str) -> bool {
    token == "AND"
}

/// Returns true if the token is equal to "OR".
pub fn is_or(token: &str) -> bool {
    token == "OR"
}

/// Returns true if the token is equal to "NOT".
pub fn is_not(token: &str) -> bool {
    token == "NOT"
}

/// Returns true if the token is equal to "(".
pub fn is_left_paren(token: &str) -> bool {
    token == "("
}

/// Returns true if the token is equal to ")".
pub fn is_right_paren(token: &str) -> bool {
    token == ")"
}

/// Returns true if the token is equal to "WHERE".
pub fn is_where(token: &str) -> bool {
    token == "WHERE"
}

/// Returns true if the token is equal to "SELECT".
pub fn is_select(token: &str) -> bool {
    token == "SELECT"
}

/// Returns true if the token is equal to "UPDATE".
pub fn is_update(token: &str) -> bool {
    token == "UPDATE"
}

/// Returns true if the token is equal to "INSERT".
pub fn is_insert(token: &str) -> bool {
    token == "INSERT"
}

/// Returns true if the token is equal to "INTO".
pub fn is_into(token: &str) -> bool {
    token == "INTO"
}

/// Returns true if the token is equal to "FROM".
pub fn is_from(token: &str) -> bool {
    token == "FROM"
}

/// Returns true if the token is equal to "ORDER".
pub fn is_order(token1: &str) -> bool {
    token1 == "ORDER"
}

/// Returns true if the token is equal to "BY".
pub fn is_by(token1: &str) -> bool {
    token1 == "BY"
}

/// Returns true if the token is equal to "DELETE".
pub fn is_delete(token: &str) -> bool {
    token == "DELETE"
}

/// Returns true if the token is equal to "SET".
pub fn is_set(token: &str) -> bool {
    token == "SET"
}

/// Returns true if the token is equal to "VALUES".
pub fn is_values(token: &str) -> bool {
    token == "VALUES"
}

/// Returns true if the token is equal to "LIMIT"
pub fn is_limit(token: &str) -> bool {
    token.eq_ignore_ascii_case("LIMIT")
}
