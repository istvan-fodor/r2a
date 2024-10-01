use anyhow::Result;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures::task::LocalSpawnExt;
use futures::{executor::LocalPool, future, stream::StreamExt};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use r2a::ArrowSupport;
use r2a::RowBuilder;
use r2r::sensor_msgs::msg::LaserScan;
use r2r::QosProfile;
use std::fs::File;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tokio::task;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
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
                // Spin the ROS2 node for processing subscriptions and publications.
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
async fn subscriber(arc_node: Arc<Mutex<r2r::Node>>) -> Result<(), r2r::Error> {
    let sub = arc_node
        .lock()
        .unwrap()
        .subscribe::<LaserScan>("/laser_scan", QosProfile::default())?;

    let fields = LaserScan::arrow_fields();
    let mut row_builder = LaserScan::new_row_builder(fields.iter().collect());
    let mut count = 0;
    sub.for_each(|msg| {
        count += 1;

        row_builder.add_row(&msg);

        if count > 0 && count % 10 == 0 {
            let arrays = row_builder.to_arc_arrays();
            let schema = Schema::new(fields.clone());
            let file_path = format!("build/laser_scan_{}.parquet", count / 10);
            write_to_parquet(arrays, Arc::new(schema), &file_path).unwrap();
            println!("Wrote data to parquet file {}", file_path)
        }

        futures::future::ready(())
    })
    .await;

    Ok(())
}

/// This publisher publishes 30 LaserScan messages
async fn publisher(arc_node: Arc<Mutex<r2r::Node>>) -> Result<(), r2r::Error> {
    let (mut timer, publisher) = {
        let mut node = arc_node.lock().unwrap();
        let timer = node.create_wall_timer(std::time::Duration::from_millis(10))?;
        let publisher = node.create_publisher::<LaserScan>("/laser_scan", QosProfile::default())?;
        (timer, publisher)
    };

    for tick in 0..30 {
        timer.tick().await?;
        let msg = LaserScan {
            header: r2r::std_msgs::msg::Header {
                stamp: r2r::builtin_interfaces::msg::Time {
                    sec: tick,
                    nanosec: 0,
                },
                frame_id: "test_frame".to_string(),
            },
            angle_min: -1.57,
            angle_max: 1.57,
            angle_increment: 0.01,
            time_increment: 0.001,
            scan_time: 0.05,
            range_min: 0.2,
            range_max: 10.0,
            ranges: vec![1.0, 2.0, 3.0, 4.0, 5.0],
            intensities: vec![0.5, 0.7, 0.9, 1.1, 1.3],
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
