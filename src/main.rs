use aws_sdk_s3::Client;
use polars_sql::*;
use polars::prelude::*;
use clap::Parser;
use tokio::task;
use std::sync::{Arc,Mutex};

 
#[derive(Parser)]
struct Cli{
    /// S3 bucket name/folder to get files
    #[arg(short = 'b', long = "sb")]
    source_bucket: String,
    
    /// S3 bucket name/folder to save processed files
    #[arg(short,long)]
    destiny_bucket: Option<String>,

    /// Query SQL, use df as DataFrame name like: "SELECT col_a FROM df"
    #[arg(short = 'q', long = "qu", default_value_t = String::from("SELECT * FROM df"))]
    query: String,

    /// Save dataframe localy
    #[arg(short, long)]
    save: bool,
}
 

 #[tokio::main]
 async fn main() -> Result<(),Box<dyn std::error::Error>> {
    // Load env args
    let cli: Cli= Cli::parse();
    
    //Load aws credentials and connect a Client to S3
    let config: aws_config::SdkConfig = aws_config::from_env().load().await;
    let client: Arc<Client> = Arc::new(Client::new(&config));

    // Create LazyFrame
    let df: LazyFrame = create_lazy_frame(Arc::clone(&client), &cli.source_bucket).await?;

    //Get SQL context 
    let mut ctx: SQLContext = SQLContext::try_new()?;

    ctx.register("df", df);

    let mut sql_df: DataFrame = ctx.execute(&cli.query).unwrap().collect().unwrap();

    println!("{:?}",sql_df);

    if cli.save{
        let mut file: std::fs::File = std::fs::File::create("df.snappy.parquet").unwrap();
        ParquetWriter::new(&mut file).with_compression(ParquetCompression::Snappy).finish(& mut sql_df)?;
    };

    match cli.destiny_bucket{
        Some(bucket_name) => copy_to_bucket(&client, &bucket_name,& mut sql_df).await?,
        None => (),
    };

    Ok(())

 }


 async fn create_lazy_frame(client:Arc<Client>, source_bucket:&str) -> Result<LazyFrame,Box<dyn std::error::Error>> {
    //Create an empty Vector to store LazyFrames 
    let dfs: Arc<Mutex<Vec<LazyFrame>>>= Arc::new(Mutex::new(Vec::new()));

    // Read parquet files to an Cursor use it to create an LazyFrame from Parquet
    //List all objects in bucket whit the chosen PREFIX
    let objects: aws_sdk_s3::output::ListObjectsV2Output = client.list_objects_v2().bucket(source_bucket).send().await?;

    //Loop throug all files and read each file to an Lazy Frame
    let mut tasks = vec![];

    for obj in objects.contents().unwrap_or_default() {
        let obj_key = obj.key().unwrap().to_owned();
        let dfs_clone = Arc::clone(&dfs);
        let source_bucket_clone = source_bucket.to_owned();
        let client_clone = Arc::clone(&client);

        let task = task::spawn(async move {
        let req: aws_sdk_s3::types::AggregatedBytes = client_clone.get_object()
                                .bucket(source_bucket_clone)
                                .key(obj_key)
                                .send()
                                .await.unwrap()
                                .body
                                .collect()
                                .await.unwrap();

        let df: LazyFrame = ParquetReader::new(std::io::Cursor::new(req.into_bytes()))
                                .finish().unwrap()
                                .lazy();
        
        //Store the LazyFrame into dfs Vec           
        dfs_clone.lock().unwrap().push(df);
        }
        );
        tasks.push(task)
    };

    for task in tasks{
        task.await?
    }
    
    let dfs_locked:Vec<LazyFrame> = dfs.lock().unwrap().clone();

    let df: LazyFrame = concat(dfs_locked,true,true)?;

    Ok(df)

 }

 async fn copy_to_bucket(client:&Client, destiny_bucket:&str, df:& mut DataFrame) -> Result<(),Box<dyn std::error::Error>> {
    
    //Create an empty cursor to store DF bytes and save them into S3
    let mut bytes: std::io::Cursor<Vec<u8>> = std::io::Cursor::new(Vec::new());

    //Write the parquet data into the bytes' cursor using Snappy compression
    ParquetWriter::new(&mut bytes).with_compression(ParquetCompression::Snappy).finish(df)?;

    // Create the ByteStream body needed to upload data to S3
    let body = aws_sdk_s3::types::ByteStream::from(bytes.into_inner());

    //Put the Object into S3
    client
        .put_object()
        .bucket(destiny_bucket)
        .key("/processed/processed_file.snappy.parquet")
        .body(body)
        .send()
        .await;    

    Ok(())
 }