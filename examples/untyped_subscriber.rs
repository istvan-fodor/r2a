use anyhow::Result;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use r2a::ArrowSupport;
use r2a::RowBuilder;
use r2r::sensor_msgs::msg::PointCloud2;
use r2r::sensor_msgs::msg::PointField;
use r2r::QosProfile;
use std::fs::File;
use std::sync::{Arc, Mutex};
use tokio::task;

/// This example demonstrates the use of the `r2a::ArrowSupport` and `r2a::RowBuilder`
/// structs with complex types, such as PointCloud2
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    unsafe { backtrace_on_stack_overflow::enable() };
    let ctx = r2r::Context::create()?;
    let node = r2r::Node::create(ctx, "consumer_node", "")?;

    let arc_node = Arc::new(Mutex::new(node));

    let topics_and_types = arc_node
        .lock()
        .unwrap()
        .get_topic_names_and_types()
        .unwrap();
    println!("Topics and types: {:?}", topics_and_types);

    for (topic, types) in topics_and_types.iter() {
        if types.len
        task::spawn(async move { subscriber(an_sub).await.unwrap() })
    }


    // let an_sub = arc_node.clone();
    // task::spawn(async move { subscriber(an_sub).await.unwrap() });

    // let an_pub = arc_node.clone();
    // task::spawn(async move { publisher(an_pub).await.unwrap() });

    // let an_spin = arc_node.clone();
    // let spin_task = task::spawn(async move {
    //     let mut i = 1;
    //     loop {
    //         {
    //             // Spin the ROS 2 node for processing subscriptions and publications.
    //             an_spin
    //                 .lock()
    //                 .unwrap()
    //                 .spin_once(std::time::Duration::from_millis(10));
    //             i += 1;
    //             if i > 100 {
    //                 break;
    //             }
    //         }
    //         // Sleep a bit to yield and let other tasks run.
    //         tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    //     }
    // });

    // spin_task.await?;

    Ok(())
}
