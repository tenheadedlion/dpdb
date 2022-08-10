#[allow(dead_code)]
pub(crate) fn eq_u8(lhs: &[u8], rhs: &[u8]) -> bool {
    if lhs.len() != rhs.len() {
        return false;
    }
    for (u1, u2) in lhs.iter().zip(rhs.iter()) {
        if u1 != u2 {
            return false;
        }
    }
    true
}
