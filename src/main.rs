use aws_sdk_s3::Client;
use polars_sql::*;
use polars::prelude::*;
use clap::Parser;
use tokio::task;
use std::sync::{Arc,Mutex};

// Defining a size to our chunks
const CHUNK_SIZE:usize = 1_000_000;

#[derive(Parser)]
struct Cli{
    /// S3 bucket name/folder to get files
    #[arg(short = 'b', long = "sb")]
    source_bucket: String,

    /// S3 bucket prefix that we want to filter
    #[arg(short = 'p', long = "prefix")]
    prefix: Option<String>,
    
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
    let df: LazyFrame = create_lazy_frame(Arc::clone(&client), &cli.source_bucket, cli.prefix).await?;

    //Get SQL context 
    let mut ctx: SQLContext = SQLContext::try_new()?;

    ctx.register("df", df);

    //Proccess data
    let mut sql_df: DataFrame = ctx.execute(&cli.query).unwrap().collect().unwrap();

    //println!("{:?}",sql_df);

    //When save flag is on
    if cli.save{
        let mut file: std::fs::File = std::fs::File::create("df.snappy.parquet").unwrap();
        ParquetWriter::new(&mut file).with_compression(ParquetCompression::Snappy).finish(& mut sql_df)?;
    };

    //When destiny bucket is set
    match cli.destiny_bucket{
        Some(bucket_name) => copy_to_bucket(Arc::clone(&client), &bucket_name,sql_df).await?,
        None => (),
    };

    Ok(())

 }


 async fn create_lazy_frame(client:Arc<Client>, source_bucket:&str, prefix:Option<String>) -> Result<LazyFrame,Box<dyn std::error::Error>> {
    // This function reads parquet data from S3, and groups that in a LazyFrame 

    //Create an Arc<Mutex<_>> from an empty Vector to store LazyFrames 
    let dfs: Arc<Mutex<Vec<LazyFrame>>>= Arc::new(Mutex::new(Vec::new()));

    //List all objects in bucket whit the chosen PREFIX
    let objects: aws_sdk_s3::output::ListObjectsV2Output = match prefix{
        Some(prefix) => client.list_objects_v2().bucket(source_bucket).prefix(prefix).send().await?,
        None => client.list_objects_v2().bucket(source_bucket).send().await?
    };

    //Loop throug all files and read each file to an Lazy Frame
    let mut tasks = vec![];

    //For each file we will spawn a thread to handle the creation of respective LazyFrame
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
    
    // Get Vec<LazyFrame> out of Arc<Mutex<_>>
    let dfs_locked:Vec<LazyFrame> = dfs.lock().unwrap().clone();

    let df: LazyFrame = concat(dfs_locked,true,true)?;

    Ok(df)

 }

async fn get_chunks(df:DataFrame) -> Vec<DataFrame>{
    let mut tasks = vec![];

    let df = Arc::from(df);

    //Crate a Vec to save the slices of Dfs throug threads
    let vec_chunks = Arc::new(Mutex::new(Vec::new()));

    //Calculate the number of chunks that whe need
    let n_chunks:usize = (df.estimated_size()/CHUNK_SIZE) + 1;

    //Calculate how many lines of the DF each chunk should have(max of lines)
    let n_lines:usize = df.shape().0/n_chunks;

    //Iterate and use threads to create the slices
    for i in 0..=n_chunks{

        let vec_chunks_clone = Arc::clone(&vec_chunks);
        let df_clone = Arc::clone(&df);
        let n_lines_clone = n_lines.clone();

        let task = task::spawn(async move{
            let sliced_df = df_clone.slice((i*n_lines_clone) as i64, n_lines_clone);

            if sliced_df.shape().0 > 0{
                vec_chunks_clone.lock().unwrap().push(sliced_df);
            }
        });

        tasks.push(task)
    };
    
    //Await all threads to finish
    for task in tasks{
        task.await.unwrap()
    };

    // Get Vec<DataFrame> out of Arc<Mutex<_>>
    let result = vec_chunks.lock().unwrap().clone();

    return result
}


 async fn copy_to_bucket(client:Arc<Client>,destiny_bucket:&str, df:DataFrame) -> Result<(),Box<dyn std::error::Error>> {
    
    //Get the Vec with ours Df's chunks
    let chunked_dfs: Vec<DataFrame> = get_chunks(df).await;
    
    let mut tasks = vec![];

    //Put a thread to upload each chunk to S3
    for (i,chunk) in chunked_dfs.into_iter().enumerate(){
        let client_clone = Arc::clone(&client);
        let destiny_bucket_clone = destiny_bucket.to_owned();
        let mut chunk = chunk.to_owned();
        //Create an empty cursor to store DF bytes and save them into S3
        let mut cursor: std::io::Cursor<Vec<u8>> = std::io::Cursor::new(Vec::new());
        let task = task::spawn(async move{

            //Write the parquet data into the bytes' cursor using Snappy compression
            ParquetWriter::new(&mut cursor).with_compression(ParquetCompression::Snappy).finish(&mut chunk).unwrap();

            // Create the ByteStream body needed to upload data to S3
            let body = aws_sdk_s3::types::ByteStream::from(cursor.into_inner());
            let filename = format!("processed/file{}.snappy.parquet",i);

            //Put the Object into S3
            client_clone
                .put_object()
                .bucket(destiny_bucket_clone)
                .key(filename)
                .body(body)
                .send()
                .await;
        });

        tasks.push(task)
    };

    for task in tasks{
        task.await?
    };
    
    Ok(())
 }