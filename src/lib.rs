// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! # R2A: Arrow bindings to ROS2
//!
//! This library provides utilities for mapping ROS messages to Arrow arrays. It heavily relies
//! on the [r2r](https://github.com/sequenceplanner/r2r) library. Much like how `r2r` works,
//! `r2a` RowBuilder and ArrowSupport implementations are automatically generated during build
//! time and includes in the compilation. Before build, you must source your ROS2 environment.
//!
//!
//! ## Features
//! - Convert ROS schema to Arrow fields.
//! - Support for flat Arrow fields mapping.
//! - A row builder for dynamic schema handling.
//! - A comprehensive list of supported ROS message schemas.
//!
//! ## Example
//! ```rust
//! use r2a::ArrowSupport;
//! use r2a::RowBuilder;
//!
//! let fields = r2r::std_msgs::msg::Header::arrow_fields();
//! let mut row_builder = r2r::std_msgs::msg::Header::new_row_builder(fields.iter().collect()); //We keep all the fields, convert to Vec<&Field>
//!
//! let my_message = r2r::std_msgs::msg::Header {
//!                stamp: r2r::builtin_interfaces::msg::Time {
//!                    sec: 0,
//!                    nanosec: 0,
//!                },
//!                frame_id: "test_frame".to_string(),
//!            };
//! row_builder.add_row(&my_message).unwrap();
//! let arrow_arrays = row_builder.to_arc_arrays();
//! ```
//!

mod ros_mapper;
mod schema;

pub use ros_mapper::ArrowSupport;
pub use ros_mapper::RowBuilder;

/// Returns an array of supported ROS message schemas. The list is automatically generated in compilation time.
pub fn get_supported_schemas() -> &'static [&'static str] {
    schema::SUPPORTED_SCHEMAS
}

#[cfg(test)]
mod tests {}
