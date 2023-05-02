use aws_sdk_s3::Client;
use polars_sql::*;
use polars::prelude::*;
use clap::Parser;

 
#[derive(Parser)]
struct Cli{
    /// S3 bucket name/folder to get files
    #[arg(short = 'b', long = "sb")]
    source_bucket: String,
    
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
    let mut ctx = SQLContext::try_new()?;

    ctx.register("df", df);

    let mut sql_df = ctx.execute(&cli.query).unwrap().collect().unwrap();

    println!("{:?}",sql_df);

    if cli.save{
        let mut file: std::fs::File = std::fs::File::create("df.snappy.parquet").unwrap();
        ParquetWriter::new(&mut file).with_compression(ParquetCompression::Snappy).finish(& mut sql_df)?;
    }

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