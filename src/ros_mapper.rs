use anyhow::Result;
use arrow_array::builder::ArrayBuilder;
use arrow_array::Array;
use std::sync::Arc;

/// The `RowBuilder` trait is implemented for each ROS 2 message type by a code generator.
/// It serves as an accumulator that collects records and converts them into a collection
/// of Arrow arrays. This trait is responsible for managing how records are added and stored
/// and how they are converted into Arrow-compatible data structures.
///
/// # Type Parameters
///
/// - `'a`: The lifetime of the references to the message and fields.
/// - `T`: The specific ROS 2 message type that the row builder will accumulate.
///
/// # Example
///
/// ```
/// use r2a::ArrowSupport;
/// use r2a::RowBuilder;
///
/// let fields = r2r::std_msgs::msg::Header::arrow_fields(false);
/// let mut row_builder = r2r::std_msgs::msg::Header::new_row_builder(fields.iter().collect()); //We keep all the fields, convert to Vec<&Field>
///
/// let my_message = r2r::std_msgs::msg::Header {
///                stamp: r2r::builtin_interfaces::msg::Time {
///                    sec: 0,
///                    nanosec: 0,
///                },
///                frame_id: "test_frame".to_string(),
///            };
/// row_builder.add_row(&my_message).unwrap();
/// let arrow_arrays = row_builder.to_arc_arrays();
/// ```
pub trait RowBuilder<'a, T> {
    /// Adds a ROS 2 message of type `T` to the row builder.
    ///
    /// This method takes a reference to the message, processes it, and stores the data
    /// internally in Arrow array builders that can later be converted to Arrow arrays.
    ///
    /// # Arguments
    ///
    /// * `msg` - A reference to a message of type `T` that will be added to the builder.
    ///
    /// # Errors
    ///
    /// This method returns a `Result` that can indicate an error if the addition of the message
    /// fails for any reason.
    fn add_row(&mut self, msg: &T) -> Result<()>;

    /// Deserializes then adds raw binary data (usually in the form of a serialized message) to the row builder.
    ///
    /// # Arguments
    ///
    /// * `msg` - A byte slice representing the raw serialized message data.
    ///
    /// # Errors
    ///
    /// This method returns a `Result` that can indicate an error if the raw data cannot be
    /// processed or added correctly.
    fn add_raw_row(&mut self, msg: &[u8]) -> Result<()>;

    /// Converts the accumulated rows into a vector of Arrow arrays and resets the internal state
    /// of the builder.
    ///
    /// This method returns the data collected by the row builder as a collection of Arrow arrays,
    /// which can be used for further processing or storage in formats like Parquet.
    ///
    /// # Returns
    ///
    /// A vector of Arrow arrays, where each array represents a column of data from the accumulated
    /// rows.
    fn to_arc_arrays(&mut self) -> Vec<Arc<dyn Array>>;
}

/// The `ArrowSupport` trait is implemented for each ROS 2 message type, allowing the creation of
/// row builders, Arrow schemas, and field definitions for that message type.
///
/// # Associated Types
///
/// - `RowBuilderType`: The type of row builder that will be used to accumulate data for the
///   implementing ROS 2 message type.
///
/// # Example
///
/// ```
/// // Assuming a type `MyROS 2Message` implements `ArrowSupport`.
/// use r2a::ArrowSupport;
///
/// let arrow_fields = r2r::std_msgs::msg::Header::arrow_fields(false);
/// let schema = r2r::std_msgs::msg::Header::arrow_schema(false);
/// let row_builder = r2r::std_msgs::msg::Header::new_row_builder(arrow_fields.iter().collect());
/// ```
pub trait ArrowSupport<'a> {
    /// The type of row builder that this ROS 2 message type will use to accumulate rows.
    /// This type is specific to the ROS 2 message type that implements the `ArrowSupport` trait.
    type RowBuilderType;

    /// The type of row builder that this ROS 2 message type will use to accumulate rows. The FlatRowBuilder will try to flatten out ROS messages as much as possible.
    /// This type is specific to the ROS 2 message type that implements the `ArrowSupport` trait.
    type FlatRowBuilderType;

    /// This method returns the name of the ROS 2 message type as a string, which can be used
    /// for identification or other purposes within the code.
    ///
    /// # Returns
    ///
    /// The name of the ROS 2 message type as a string.
    fn schema_name() -> &'static str;

    /// Creates a new row builder for the given ROS 2 message type.
    ///
    /// This method creates a row builder using the provided Arrow field definitions. The row
    /// builder is then used to accumulate data for conversion to Arrow arrays.
    ///
    /// # Arguments
    ///
    /// * `arrow_fields` - A vector of references to Arrow field definitions that specify the
    ///   structure of the data for this ROS 2 message type. This has to be a subset of fields
    ///   returned by the `arrow_fields` method.
    ///
    /// # Returns
    ///
    /// A row builder of type `RowBuilderType`, which can be used to accumulate rows for the
    /// implementing ROS 2 message type.
    fn new_row_builder(arrow_fields: Vec<&'a arrow_schema::Field>) -> Self::RowBuilderType;

    /// Creates a new row builder for the given ROS 2 message type.
    ///
    /// This method creates a row builder using the provided Arrow field definitions. The row
    /// builder is then used to accumulate data for conversion to Arrow arrays.
    ///
    /// # Arguments
    ///
    /// * `arrow_fields` - A vector of references to Arrow field definitions that specify the
    ///   structure of the data for this ROS 2 message type. This has to be a subset of fields
    ///   returned by the `arrow_fields` method.
    ///
    /// # Returns
    ///
    /// A row builder of type `RowBuilderType`, which can be used to accumulate rows for the
    /// implementing ROS 2 message type.
    fn new_flat_row_builder(arrow_fields: Vec<&'a arrow_schema::Field>)
        -> Self::FlatRowBuilderType;

    /// Returns the Arrow field definitions for this ROS 2 message type.
    ///
    /// This method returns the Arrow field definitions that describe the structure of the ROS 2
    /// message type.
    ///
    /// # Arguments
    ///
    /// * `include_msg_struct` - If true, the field `message_struct` will be included. The field
    /// contains the whole message as an Arrow StructArray. This is useful when having the message
    /// available in a single column is desirable, such as in the case of a downstream JSON
    /// conversion. If false, the `message_struct` field will not be included.
    ///
    /// # Returns
    ///
    /// A vector of Arrow field definitions (`arrow_schema::Field`) for the ROS 2
    /// message type.
    fn arrow_fields(include_msg_struct: bool) -> Vec<arrow_schema::Field>;

    /// Returns the Arrow schema for this ROS 2 message type.
    ///
    /// This method generates the complete Arrow schema for the ROS 2 message type, which can be
    /// used when creating Arrow arrays or converting the data to other formats like Parquet.
    ///
    /// # Arguments
    ///
    /// * `include_msg_struct` - If true, the field `message_struct` will be included. The field
    /// contains the whole message as an Arrow StructArray. This is useful when having the message
    /// available in a single column is desirable, such as in the case of a downstream JSON
    /// conversion. If false, the `message_struct` field will not be included.
    ///
    /// # Returns
    ///
    /// An Arrow schema (`arrow_schema::Schema`) that represents the full structure of the ROS 2
    /// message type plus the optional `message_struct` field.
    fn arrow_schema(include_msg_struct: bool) -> arrow_schema::Schema;

    /// Returns the Arrow schema for this ROS 2 message type.
    ///
    /// This method generates the complete a flattened Arrow schema for the ROS 2 message type, which can be
    /// used when creating Arrow arrays or converting the data to other formats like Parquet.
    ///
    /// Structs are flattened out as much as possible, with the exception of arrays, which are
    /// represented as a LongList of of their type. With this transformation the message record loses the
    /// exact structural match from the original ROS 2 message type but the format lends itself well for analytics
    /// usecases (SQL, etc) where easy access to embedded fields is beneficial.
    ///
    /// # Arguments
    ///
    /// * `include_msg_struct` - If true, the field `message_struct` will be included. The field
    /// contains the whole message as an Arrow StructArray. This is useful when having the message
    /// available in a single column is desirable, such as in the case of a downstream JSON
    /// conversion. If false, the `message_struct` field will not be included.
    ///
    /// # Returns
    ///
    /// An Arrow schema (`arrow_schema::Schema`) that represents the full structure of the ROS 2
    /// message type plus the optional `message_struct` field.
    fn flat_arrow_fields(include_msg_struct: bool) -> Vec<arrow_schema::Field>;

    /// Returns the Arrow schema for this ROS 2 message type.
    ///
    /// This method generates the complete a flattened Arrow schema for the ROS 2 message type, which can be
    /// used when creating Arrow arrays or converting the data to other formats like Parquet.
    ///
    /// For rationale see `flat_arrow_fields` documentation.
    ///
    /// # Arguments
    ///
    /// * `include_msg_struct` - If true, the field `message_struct` will be included. The field
    /// contains the whole message as an Arrow StructArray. This is useful when having the message
    /// available in a single column is desirable, such as in the case of a downstream JSON
    /// conversion. If false, the `message_struct` field will not be included.
    ///
    /// # Returns
    ///
    /// An Arrow schema (`arrow_schema::Schema`) that represents the full structure of the ROS 2
    /// message type plus the optional `message_struct` field.
    fn flat_arrow_schema(include_msg_struct: bool) -> arrow_schema::Schema;
}

#[cfg(feature = "default")]
include!(concat!(env!("OUT_DIR"), "/generated_arrow_mappers.rs"));

#[cfg(test)]
mod tests {

    use super::ArrowSupport;
    use super::RowBuilder;
    use r2r::builtin_interfaces::msg::Time;
    use r2r::std_msgs::msg::Header;

    #[test]
    fn test_append_and_to_array() {
        let mut v = Vec::with_capacity(100);
        for _ in 0..100 {
            v.push(Header {
                stamp: Time { sec: 0, nanosec: 0 },
                frame_id: "test_frame".to_string(),
            });
        }

        let fields = Header::arrow_fields(true);
        let fields = fields.iter().filter(|f| f.name() == "stamp").collect();

        let mut row_builder = Header::new_row_builder(fields);
        for msg in v.iter() {
            assert!(row_builder.add_row(msg).is_ok());
        }

        let arrays = row_builder.to_arc_arrays();
        assert_eq!(arrays.len(), 1);
        assert_eq!(arrays[0].len(), 100);
        assert_eq!(arrays[0].null_count(), 0);

        let is_correct_struct = match arrays[0].data_type() {
            arrow_schema::DataType::Struct(fields) => {
                fields.len() == 2
                    && fields.get(0).unwrap().name() == "sec"
                    && fields.get(0).unwrap().data_type() == &arrow_schema::DataType::Int32
                    && fields.get(1).unwrap().name() == "nanosec"
                    && fields.get(1).unwrap().data_type() == &arrow_schema::DataType::UInt32
            }
            _ => false,
        };
        assert!(is_correct_struct);
    }

    #[test]
    fn test_append_and_to_array_flat() {
        let mut v = Vec::with_capacity(100);
        for _ in 0..100 {
            v.push(Header {
                stamp: Time { sec: 0, nanosec: 0 },
                frame_id: "test_frame".to_string(),
            });
        }

        let fields = Header::flat_arrow_fields(true);

        let fields = fields
            .iter()
            .filter(|f| f.name() == "stamp_sec" || f.name() == "message_struct")
            .collect();

        let mut row_builder = Header::new_flat_row_builder(fields);
        for msg in v.iter() {
            assert!(row_builder.add_row(msg).is_ok());
        }

        let arrays = row_builder.to_arc_arrays();
        assert_eq!(arrays.len(), 2);
        assert_eq!(arrays[0].len(), 100);
        assert_eq!(arrays[0].null_count(), 0);

        assert!(arrays[0].data_type() == &arrow_schema::DataType::Int32);

        let is_correct_struct = match arrays[1].data_type() {
            arrow_schema::DataType::Struct(fields) => {
                assert!(fields.get(0).unwrap().name() == "stamp");
                match fields.get(0).unwrap().data_type() {
                    arrow_schema::DataType::Struct(fields) => {
                        fields.len() == 2
                            && fields.get(0).unwrap().name() == "sec"
                            && fields.get(0).unwrap().data_type() == &arrow_schema::DataType::Int32
                            && fields.get(1).unwrap().name() == "nanosec"
                            && fields.get(1).unwrap().data_type() == &arrow_schema::DataType::UInt32
                    }
                    _ => false,
                }
            }
            _ => false,
        };
        assert!(is_correct_struct);
    }
}
