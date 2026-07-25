use std::{collections::HashMap, sync::Arc};

use tracing::warn;

pub type Symbol = Arc<str>;

/// Upper bound on interned entries. Keys come from exchange-controlled strings
/// (a Binance `stream` name, a Hyperliquid `coin`), so an unexpected feed must
/// not be able to grow this map without limit. Past the cap the cache still
/// returns correct symbols, it just stops memoising them.
const MAX_INTERNED: usize = 4_096;

pub struct SymbolCache {
    symbols: HashMap<String, Symbol>,
    last: Option<Symbol>,
    capacity_warned: bool,
}

impl SymbolCache {
    pub fn new(symbols: &[String]) -> Self {
        let mut cache = Self {
            symbols: HashMap::with_capacity(symbols.len() * 2),
            last: None,
            capacity_warned: false,
        };
        for symbol in symbols {
            let normalized: Symbol = Arc::from(symbol.to_ascii_lowercase());
            cache
                .symbols
                .insert(symbol.clone(), Arc::clone(&normalized));
            cache.symbols.insert(normalized.to_string(), normalized);
        }
        cache
    }

    pub fn resolve(&mut self, raw: &str) -> Symbol {
        if let Some(symbol) = &self.last
            && raw.eq_ignore_ascii_case(symbol)
        {
            return Arc::clone(symbol);
        }

        if let Some(symbol) = self.symbols.get(raw) {
            let symbol = Arc::clone(symbol);
            self.last = Some(Arc::clone(&symbol));
            return symbol;
        }

        // Probe the canonical key before allocating, so a casing this cache was
        // not seeded with still resolves to the existing `Arc` instead of
        // minting a second one with identical contents.
        let normalized = raw.to_ascii_lowercase();
        if let Some(symbol) = self.symbols.get(normalized.as_str()) {
            let symbol = Arc::clone(symbol);
            self.remember(raw.to_owned(), &symbol);
            self.last = Some(Arc::clone(&symbol));
            return symbol;
        }

        let symbol: Symbol = Arc::from(normalized.as_str());
        self.remember(normalized, &symbol);
        self.remember(raw.to_owned(), &symbol);
        self.last = Some(Arc::clone(&symbol));
        symbol
    }

    fn remember(&mut self, key: String, symbol: &Symbol) {
        if self.symbols.len() >= MAX_INTERNED {
            if !self.capacity_warned {
                self.capacity_warned = true;
                warn!(
                    interned = self.symbols.len(),
                    "symbol cache is full; further symbols will not be interned"
                );
            }
            return;
        }
        self.symbols.insert(key, Arc::clone(symbol));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_canonical_symbol_for_exchange_case_variants() {
        let mut cache = SymbolCache::new(&["BTCUSDT".to_owned()]);
        let upper = cache.resolve("BTCUSDT");
        let lower = cache.resolve("btcusdt");

        assert!(Arc::ptr_eq(&upper, &lower));
        assert_eq!(upper.as_ref(), "btcusdt");
    }

    #[test]
    fn miss_interns_future_case_variants() {
        let mut cache = SymbolCache::new(&[]);
        let mixed = cache.resolve("BtcUsdt");
        let _ = cache.resolve("ETHUSDT");
        let lower = cache.resolve("btcusdt");
        let _ = cache.resolve("ETHUSDT");
        // A casing that was never inserted still finds the canonical entry.
        let upper = cache.resolve("BTCUSDT");

        assert!(Arc::ptr_eq(&mixed, &lower));
        assert!(Arc::ptr_eq(&mixed, &upper));
    }

    #[test]
    fn interning_is_bounded() {
        let mut cache = SymbolCache::new(&[]);
        for index in 0..MAX_INTERNED {
            let _ = cache.resolve(&format!("sym{index}"));
        }

        assert!(cache.symbols.len() <= MAX_INTERNED);
        // Still correct past the cap, just no longer memoised.
        assert_eq!(cache.resolve("LATEUSDT").as_ref(), "lateusdt");
    }
}
