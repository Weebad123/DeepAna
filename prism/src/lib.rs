mod commitment;
pub mod types;
mod storage;
mod normalizer;
mod decoder;
mod ingestor;
mod slot_processor;
mod adapter;
mod account_resolver;
mod changelog;

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
