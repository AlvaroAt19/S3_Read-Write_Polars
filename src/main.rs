use aws_sdk_s3::Client;
use polars_sql::*;
use polars::prelude::*;

//Definindo nosso bucket de origem
const SOURCE_BUCKET:&str = "bucket-rust-polars-46212";
//Nesse caso, o bucket de destino será o mesmo que o de origem para simplificar as coisas
//Porém os dados processados serão salvos em uma pasta diferente
const DESTINY_BUCKET:&str = "bucket-rust-polars-46212";

#[tokio::main]
 async fn main() -> Result<(),Box<dyn std::error::Error>> {
    
    //Carregando as credenciais da AWS presentes no arquivo ~/.aws/credentials
    let config: aws_config::SdkConfig = aws_config::from_env().region("us-east-2").load().await;
    
    //Instanciando um Client a partir das credenciais
    let client: Client = Client::new(&config);
    
    //Listando todos os objetos no SOURCE_BUCKET
    let objects: aws_sdk_s3::output::ListObjectsV2Output = client.list_objects_v2().bucket(SOURCE_BUCKET).send().await?;
    
    //Criando um Vec para armazenar os arquivos lidos e no final agrupa-los em um único DataFrame
    let mut dfs: Vec<LazyFrame> = Vec::new();
    
    //Fazendo um looping para assim ler todos os objetos presentes no bucket
    for obj in objects.contents().unwrap_or_default() {
        
        //Aqui é feita a requisição de GetObject para o S3 que irá nos retornar um AggregatedBytes
        let req: aws_sdk_s3::types::AggregatedBytes = client.get_object()
                                .bucket(SOURCE_BUCKET)
                                .key(obj.key().unwrap())
                                .send()
                                .await?
                                .body
                                .collect()
                                .await?;
        
        //Para utilizarmos o ParquetReader do Polars, necessitamos ou de um arquivo indicado para ser lido,
        // ou de um Cursor que é um buffer em memória para realizar operações de escrita e leitura
        let df: LazyFrame = ParquetReader::new(std::io::Cursor::new(req.into_bytes()))
                                .finish()?
                                .lazy();
        
        //Por final, adicionamos o df lido no vetor de dfs, para agrega-los posteriormente
        dfs.push(df)
    };

    //Agora concatenamos todos os dataframes em um único
    let df: LazyFrame = concat(dfs,true,true)?;

    //Com os dados completos carregados, podemos executar operações a partir da API de DataFrame
    //ou podemos utilizar a query engine e rodar comandos SQL
    //Nesse caso, vamos utilizar a query engine
    //Primeiro passo é instanciar um SQLContext
    let mut ctx: SQLContext = SQLContext::try_new()?;
    
    //E por fim registrar o df com o nome desejado, nesse caso optei por manter df
    ctx.register("df", df.clone());

    //Agora podemos processar os dados executando queries
    let mut sql_df: DataFrame = ctx.execute("SELECT SQUARE_FT, ADDRESS from df").unwrap().collect().unwrap();

    //Para exibir os dados, fazemos esse print
    println!("{:?}",sql_df);

    
    //Agora processando no formato DataFrame
    let df_new: DataFrame = df.filter(col("SQUARE_FT").gt(500.0)).collect()?;
    println!("{:?}",df_new);
    
    
    //Assim como para ler os arquivos criamos um cursor de bytes a partir de AggregatedBytes
    //Para escrever no S3 faremos o caminho inverso, sendo ele:
    
    //Criamos um cursor a partir de um vetor de bytes vazio
    let mut cursor: std::io::Cursor<Vec<u8>> = std::io::Cursor::new(Vec::new());
    
    //Escrevemos no cursor os dados processados(sql_df) como Parquet, e comprimidos como Snappy
    ParquetWriter::new(&mut cursor).with_compression(ParquetCompression::Snappy).finish(&mut sql_df).unwrap();

    //Transformamos o cursor de bytes em um ByteStream
    let body: aws_sdk_s3::types::ByteStream = aws_sdk_s3::types::ByteStream::from(cursor.into_inner());

    //Definimos um nome para o arquivo
    let filename: &str = "processed/example_file.snappy.parquet";

    //Utilizamos o SDK agora com a chamada de PutObject no S3
    let response: aws_sdk_s3::output::PutObjectOutput = client
        .put_object()
        .bucket(DESTINY_BUCKET)
        .key(filename)
        .body(body)
        .send()
        .await?;


    Ok(())
 }