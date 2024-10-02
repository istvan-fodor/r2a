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

//! # R2A: Arrow bindings to ROS 2
//!
//! This library provides utilities for mapping ROS messages to Arrow arrays. It heavily relies
//! on the [r2r](https://github.com/sequenceplanner/r2r) library. Much like how `r2r` works,
//! `r2a` RowBuilder and ArrowSupport implementations are automatically generated during build
//! time and includes in the compilation. Before build, you must source your ROS 2 environment.
//!
//!
//! ## Features
//! - Convert ROS schema to Arrow fields.
//! - Support for 1-1 match and flat Arrow fields mapping. 1-1 match follows the exact structure of the original ROS message, while flat is a "more tabular" format.
//! - A row builder for storing converted rows.
//! - All ROS message schemas are supported as long as they are properly sourced.
//!
//! ## Example
//! ```rust
//! use r2a::ArrowSupport;
//! use r2a::RowBuilder;
//!
//! let fields = r2r::std_msgs::msg::Header::arrow_fields(true); // If parameter is true, we also store an extra column called `message struct` that will include the complete message as a struct.
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
//! //store to parquet, etc.
//! ```
//!
//! For more elaborate examples, see the [examples in the Git repo](https://github.com/istvan-fodor/r2a/tree/main/examples).
//!
//! When we create fields with `arrow_fields` and the RowBuilder `new_row_builder`, the message will keep it's original structure. For example a `sensor_msgs/msg/LaserScan` message will be converted to Arrow like this:
//!
//! | header                                                        |   angle_min |   angle_max |   angle_increment |   time_increment |   scan_time |   range_min |   range_max | ranges           | intensities           | message_struct                                                                                       |
//! |:--------------------------------------------------------------|------------:|------------:|------------------:|-----------------:|------------:|------------:|------------:|:-----------------|:----------------------|:-----------------------------------------------------------------------------------------------------|
//! | {'stamp': {'sec': 0, 'nanosec': 0}, 'frame_id': 'test_frame'} |       -1.57 |        1.57 |              0.01 |            0.001 |        0.05 |         0.2 |          10 | [1. 2. 3. 4. 5.] | [0.5 0.7 0.9 1.1 1.3] | {'header': {'stamp': {'sec': 22, 'nanosec': 0}, 'frame_id': 'test_frame'}, 'angle_min': -1.570, ...} |
//! | {'stamp': {'sec': 1, 'nanosec': 0}, 'frame_id': 'test_frame'} |       -1.57 |        1.57 |              0.01 |            0.001 |        0.05 |         0.2 |          10 | [1. 2. 3. 4. 5.] | [0.5 0.7 0.9 1.1 1.3] | {'header': {'stamp': {'sec': 28, 'nanosec': 0}, 'frame_id': 'test_frame'}, 'angle_min': -1.570, ...} |
//!
//!
//! We can contrast that with `flat_arrow_fields` and `new_flat_row_builder`, there the same type of message will be converted like this:
//!
//! |   header_stamp_sec|   header_stamp_nanosec | header_frame_id   |   angle_min |   angle_max |   angle_increment |   time_increment |   scan_time |   range_min |   range_max | ranges           | intensities           | message_struct                                                                                      |
//! |------------------:|-----------------------:|:------------------|------------:|------------:|------------------:|-----------------:|------------:|------------:|------------:|:-----------------|:----------------------|:----------------------------------------------------------------------------------------------------|
//! |                 0 |                      0 | test_frame        |       -1.57 |        1.57 |              0.01 |            0.001 |        0.05 |         0.2 |          10 | [1. 2. 3. 4. 5.] | [0.5 0.7 0.9 1.1 1.3] | {'header': {'stamp': {'sec': 25, 'nanosec': 0}, 'frame_id': 'test_frame'}, 'angle_min': -1.570, ...}|
//! |                 1 |                      0 | test_frame        |       -1.57 |        1.57 |              0.01 |            0.001 |        0.05 |         0.2 |          10 | [1. 2. 3. 4. 5.] | [0.5 0.7 0.9 1.1 1.3] | {'header': {'stamp': {'sec': 23, 'nanosec': 0}, 'frame_id': 'test_frame'}, 'angle_min': -1.570, ...}|
//!
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
