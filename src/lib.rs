pub mod postgres;
pub mod sqlite;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Row {
    pub id: i32,
    pub name: String,
}

pub fn separated(s: &str, n: usize) -> String {
    let mut l = String::with_capacity(s.len() * n + 2 * n.saturating_sub(1));
    for i in 0..n {
        if i > 0 {
            l.push_str(", ");
        }
        l.push_str(s);
    }
    l
}
