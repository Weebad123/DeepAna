
pub mod api;
pub mod catalog;
pub mod detect;
pub mod  digest;
pub mod intent;
pub mod profile;
pub mod quality;
pub mod spec;
pub mod store;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
