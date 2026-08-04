pub(super) fn is_no_match_error(error: &str) -> bool {
    error.contains("no orders found to match with FAK order")
}

pub(super) fn is_balance_not_settled_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("not enough balance / allowance")
        || (error.contains("balance: 0") && error.contains("order amount"))
        || (error.contains("balance is not enough") && error.contains("allowance"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_expected_exchange_errors() {
        assert!(is_no_match_error("no orders found to match with FAK order"));
        assert!(is_balance_not_settled_error(
            "not enough balance / allowance"
        ));
        assert!(!is_balance_not_settled_error("temporary server error"));
    }
}
