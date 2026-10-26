pub mod commitment;
pub mod types;
mod storage;
pub mod normalizer;
pub mod decoder;
pub mod ingestor;
pub mod slot_processor;
pub mod adapter;
pub mod account_resolver;
pub mod changelog;

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
