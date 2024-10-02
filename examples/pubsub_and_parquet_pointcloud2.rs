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
    let node = r2r::Node::create(ctx, "testnode", "")?;
    let arc_node = Arc::new(Mutex::new(node));
    let an_sub = arc_node.clone();
    task::spawn(async move { subscriber(an_sub).await.unwrap() });

    let an_pub = arc_node.clone();
    task::spawn(async move { publisher(an_pub).await.unwrap() });

    let an_spin = arc_node.clone();
    let spin_task = task::spawn(async move {
        let mut i = 1;
        loop {
            {
                // Spin the ROS 2 node for processing subscriptions and publications.
                an_spin
                    .lock()
                    .unwrap()
                    .spin_once(std::time::Duration::from_millis(10));
                i += 1;
                if i > 100 {
                    break;
                }
            }
            // Sleep a bit to yield and let other tasks run.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    spin_task.await?;

    Ok(())
}

///
/// This subscriber will write the received LaserScan messages to parquet files in batches of 10.
///
async fn subscriber(arc_node: Arc<Mutex<r2r::Node>>) -> Result<()> {
    let sub = arc_node
        .lock()
        .unwrap()
        .subscribe::<PointCloud2>("/point_cloud2", QosProfile::default())?;

    let fields = PointCloud2::arrow_fields(true);
    let mut row_builder = PointCloud2::new_row_builder(fields.iter().collect());

    let flat_fields = PointCloud2::flat_arrow_fields(true);
    let mut flat_row_builder = PointCloud2::new_flat_row_builder(flat_fields.iter().collect());

    let mut count = 0;
    sub.for_each(|msg| {
        count += 1;

        match row_builder.add_row(&msg) {
            Ok(_) => {}
            Err(e) => {
                panic!("Error adding row to row_builder: {}", e);
            }
        }

        match flat_row_builder.add_row(&msg) {
            Ok(_) => {}
            Err(e) => {
                panic!("Error adding row to flat_row_builder: {}", e);
            }
        }

        if count > 0 && count % 10 == 0 {
            let arrays = row_builder.to_arc_arrays();
            let schema = Schema::new(fields.clone());
            let file_path = format!("target/point_cloud2_{}.parquet", count / 10);
            write_to_parquet(arrays, Arc::new(schema), &file_path).unwrap();

            println!("Wrote data to parquet file {}", file_path);

            let arrays = flat_row_builder.to_arc_arrays();

            let schema = Schema::new(flat_fields.clone());
            let file_path = format!("target/point_cloud2_flat_{}.parquet", count / 10);
            write_to_parquet(arrays, Arc::new(schema), &file_path).unwrap();

            println!("Wrote flat data to parquet file {}", file_path);
        }

        futures::future::ready(())
    })
    .await;

    Ok(())
}

/// This publisher publishes 30 PointCloud2 messages
async fn publisher(arc_node: Arc<Mutex<r2r::Node>>) -> Result<()> {
    let (mut timer, publisher) = {
        let mut node = arc_node.lock().unwrap();
        let timer = node.create_wall_timer(std::time::Duration::from_millis(10))?;
        let publisher =
            node.create_publisher::<PointCloud2>("/point_cloud2", QosProfile::default())?;
        (timer, publisher)
    };

    for tick in 0..30 {
        timer.tick().await?;
        let msg = PointCloud2 {
            header: r2r::std_msgs::msg::Header {
                stamp: r2r::builtin_interfaces::msg::Time {
                    sec: tick,
                    nanosec: 0,
                },
                frame_id: "base_link".to_string(),
            },
            height: 1, // Unordered point cloud
            width: 5,  // Number of points
            fields: vec![
                PointField {
                    name: "x".to_string(),
                    offset: 0,
                    datatype: 7, // FLOAT32
                    count: 1,
                },
                PointField {
                    name: "y".to_string(),
                    offset: 4,
                    datatype: 7, // FLOAT32
                    count: 1,
                },
                PointField {
                    name: "z".to_string(),
                    offset: 8,
                    datatype: 7, // FLOAT32
                    count: 1,
                },
                PointField {
                    name: "intensity".to_string(),
                    offset: 12,
                    datatype: 7, // FLOAT32
                    count: 1,
                },
            ],
            is_bigendian: false,
            point_step: 16,   // 4 bytes each for x, y, z, intensity
            row_step: 5 * 16, // 5 points * point_step
            data: vec![
                0, 0, 128, 63, // x = 1.0 (in IEEE 754)
                0, 0, 0, 64, // y = 2.0
                0, 0, 64, 64, // z = 3.0
                0, 0, 0,
                63, // intensity = 0.5
                    // Repeat similar data for other points
            ],
            is_dense: true,
        };

        publisher.publish(&msg)?;
    }

    Ok(())
}

fn write_to_parquet(
    arrays: Vec<Arc<dyn Array>>,
    schema: Arc<Schema>,
    file_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let file = File::create(file_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
