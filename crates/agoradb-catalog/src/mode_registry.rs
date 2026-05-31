// Copyright 2025 The AgoraDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use agoradb_core::Mode;
use std::collections::HashMap;

/// Status of an auxiliary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexStatus {
    Building,
    Ready,
    Stale,
}

/// Entry for a single mode in the Mode Registry.
#[derive(Debug, Clone)]
pub struct ModeEntry {
    pub mode_type: Mode,
    pub table_name: String,
    pub indexes: Vec<(String, IndexStatus)>,
}

/// Registry of all modes within a Space.
#[derive(Debug, Clone, Default)]
pub struct ModeRegistry {
    modes: HashMap<String, ModeEntry>,
}

impl ModeRegistry {
    pub fn new() -> Self {
        Self {
            modes: HashMap::new(),
        }
    }

    pub fn register_mode(&mut self, name: impl Into<String>, entry: ModeEntry) {
        self.modes.insert(name.into(), entry);
    }

    pub fn get_mode(&self, name: &str) -> Option<&ModeEntry> {
        self.modes.get(name)
    }

    pub fn list_modes(&self) -> Vec<&ModeEntry> {
        self.modes.values().collect()
    }

    pub fn remove_mode(&mut self, name: &str) -> Option<ModeEntry> {
        self.modes.remove(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get_mode() {
        let mut registry = ModeRegistry::new();
        let entry = ModeEntry {
            mode_type: Mode::Table,
            table_name: "posts".to_string(),
            indexes: vec![],
        };
        registry.register_mode("posts", entry.clone());
        assert!(registry.get_mode("posts").is_some());
        assert_eq!(registry.get_mode("posts").unwrap().table_name, "posts");
        assert!(registry.get_mode("nonexistent").is_none());
    }

    #[test]
    fn test_list_modes() {
        let mut registry = ModeRegistry::new();
        registry.register_mode(
            "posts",
            ModeEntry {
                mode_type: Mode::Table,
                table_name: "posts".to_string(),
                indexes: vec![],
            },
        );
        registry.register_mode(
            "graph",
            ModeEntry {
                mode_type: Mode::Graph,
                table_name: "social".to_string(),
                indexes: vec![],
            },
        );
        assert_eq!(registry.list_modes().len(), 2);
    }
}
