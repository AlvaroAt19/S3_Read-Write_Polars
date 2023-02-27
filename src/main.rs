use aws_sdk_s3::Client;
use polars::prelude::*;
 
 
 #[tokio::main]
 async fn main() -> Result<(),Box<dyn std::error::Error>> {
     //Define source bucket name
     let source_bucket:&str = "SOURCE_BUCKET_NAME";
     //Define destiny bucket name
     let destiny_bucket:&str = "DESTINY_BUCKET_NAME";
 
     //Load aws credentials and connect a Client to S3
     let config = aws_config::from_env().load().await;
     let client = Client::new(&config);
 
     //Create an empty Vector to store LazyFrames 
     let mut dfs= Vec::new();
 
     // Read parquet files to an Cursor use it to create an LazyFrame from Parquet
     {   
         //List all objects in bucket whit the chosen PREFIX
         let objects = client.list_objects_v2().bucket(source_bucket).prefix("PREFIX").send().await?;
         //Loop throug all files and read each file to an Lazy Frame
         for obj in objects.contents().unwrap_or_default() {
             let req = client.get_object()
                                     .bucket(source_bucket)
                                     .key(obj.key().unwrap())
                                     .send()
                                     .await?
                                     .body
                                     .collect()
                                     .await?;
 
             let df = ParquetReader::new(std::io::Cursor::new(req.into_bytes()))
                                     .finish()?
                                     .lazy();
             
             //Store the LazyFrame into dfs Vec           
             dfs.push(df);
         };
     };
     //Concat all LazyFrames into one LazyFrame
     let df = concat(dfs,true,true)?;
 
     //Do data transformation
     let mut df = df.collect()?;
 
     //Create an empty cursor to store DF bytes and save them into S3
     let mut bytes = std::io::Cursor::new(Vec::new());
 
     //Write the parquet data into the bytes' cursor using Snappy compression
     ParquetWriter::new(&mut bytes).with_compression(ParquetCompression::Snappy).finish(& mut df)?;
 
     // Create the ByteStream body needed to upload data to S3
     let body = aws_sdk_s3::types::ByteStream::from(bytes.into_inner());
 
     //Put the Object into S3
     client
         .put_object()
         .bucket(destiny_bucket)
         .key("KEY/FILENAME.snappy.parquet")
         .body(body)
         .send()
         .await;    
     
 
     //Print DataFrame (For Debug)
     //println!("{:?}",df);
 
     Ok(())
 }