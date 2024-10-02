R2A - A Rust library that provides a typesafe way to convert ROS 2 messages into Apache Arrow format
=================================================================================================

The library is essentially a wrapper on top of the amazing [R2R](https://github.com/sequenceplanner/r2r/) library. The two main components are `r2a::ArrowSupport` and `r2a::RowBuilder`. 

This build was mainly tested with ROS 2 Humble. I also tested it with Jazzy locally (by switching to the master branch `r2r`).

Documentation can be found on docs.rs: https://docs.rs/r2a/latest/r2a/

The crate can be found on crates.io: https://crates.io/crates/r2a 

## Install

1. `cargo add r2a`
2. Make sure to source your ROS 2 environment before you build your project. 

## Development

If you use Visual Studio Code, the `r2a.code-workspace` will be useful. If you are using a Mac, the Code workspace assumes that you have [Robostack](https://robostack.github.io/GettingStarted.html) installed and your env is called `ros_env`. 

## Examples

```rust
use r2a::ArrowSupport;
use r2a::RowBuilder;

let fields = r2r::std_msgs::msg::Header::arrow_fields();
let mut row_builder = r2r::std_msgs::msg::Header::new_row_builder(fields.iter().collect()); //We keep all the fields, convert to Vec<&Field>

let my_message = r2r::std_msgs::msg::Header {
               stamp: r2r::builtin_interfaces::msg::Time {
                   sec: 0,
                   nanosec: 0,
               },
               frame_id: "test_frame".to_string(),
           };
row_builder.add_row(&my_message).unwrap();
let arrow_arrays = row_builder.to_arc_arrays();
// store arrow_arrays as Parquet, etc..
```

For more elaborate examples see the `examples` folder.
