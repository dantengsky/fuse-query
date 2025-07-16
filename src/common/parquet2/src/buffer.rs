// Copyright 2023 Datafuse Labs.
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

/// A buffer for decompressed data
#[derive(Debug)]
pub struct DecompressedBuffer {
    /// The buffer data
    buffer: Vec<u8>,
}

impl DecompressedBuffer {
    /// Create a new decompressed buffer
    pub fn new(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }

    /// Get the buffer data as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    /// Get the buffer data as a slice
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Get the buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
