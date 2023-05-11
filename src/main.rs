use aws_sdk_s3::Client;
use polars_sql::*;
use polars::prelude::*;
use clap::Parser;

 
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
    let cli: Cli = Cli::parse();
    
    //Load aws credentials and connect a Client to S3
    let config: aws_config::SdkConfig = aws_config::from_env().load().await;
    let client: Client = Client::new(&config);

    // Create LazyFrame
    let df: LazyFrame = create_lazy_frame(&client, &cli.source_bucket).await?;

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


 async fn create_lazy_frame(client:&Client, source_bucket:&str) -> Result<LazyFrame,Box<dyn std::error::Error>> {
    //Create an empty Vector to store LazyFrames 
    let mut dfs: Vec<LazyFrame>= Vec::new();

    // Read parquet files to an Cursor use it to create an LazyFrame from Parquet
    {   
        //List all objects in bucket whit the chosen PREFIX
        let objects: aws_sdk_s3::output::ListObjectsV2Output = client.list_objects_v2().bucket(source_bucket).send().await?;
        //Loop throug all files and read each file to an Lazy Frame
        for obj in objects.contents().unwrap_or_default() {
            let req: aws_sdk_s3::types::AggregatedBytes = client.get_object()
                                    .bucket(source_bucket)
                                    .key(obj.key().unwrap())
                                    .send()
                                    .await?
                                    .body
                                    .collect()
                                    .await?;

            let df: LazyFrame = ParquetReader::new(std::io::Cursor::new(req.into_bytes()))
                                    .finish()?
                                    .lazy();
            
            //Store the LazyFrame into dfs Vec           
            dfs.push(df);
        };
    };
    
    let df: LazyFrame = concat(dfs,true,true)?;

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
        .key("/processed.snappy.parquet")
        .body(body)
        .send()
        .await;    

    Ok(())
 }