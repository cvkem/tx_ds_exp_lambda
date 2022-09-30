use lambda_runtime::{run, service_fn, Error, LambdaEvent};

use serde::{Deserialize, Serialize};

use std::time::{Duration, Instant};
use std::collections::HashMap;
// use futures::stream::futures_unordered::FuturesUnordered;
// use futures::StreamExt;
// use std::pin::Pin;
// use tokio::prelude::future::Future::poll;
//use std::future::Future;
//use std::task::Context;
//use core::future::Future;
use futures::future::try_join_all;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{Client, Error as DdbError,
//            types::SdkError,
            model::AttributeValue,
//            output::PutItemOutput,
//            error::PutItemError
};

//const NUM_REQUEST: i32 = 100;
const NUM_TX_PER_REQUEST: i32 = 1;  // in code
const ACCOUNT_NR: &str = "12345";

/// This is a made-up example. Requests come into the runtime as unicode
/// strings in json format, which can map to any structure that implements `serde::Deserialize`
/// The runtime pays no attention to the contents of the request payload.
#[derive(Deserialize)]
struct Request {
    num_request: i32,
    multiple_account: bool,
}

/// This is a made-up example of what a response structure may look like.
/// There is no restriction on what it can be. The runtime requires responses
/// to be serialized into json. The runtime pays no attention
/// to the contents of the response payload.
#[derive(Serialize)]
struct Response {
    req_id: String,
    msg: String,
}

async fn add_item(
    client: &Client,
    table: &str,
    account_nr: String,
    //datetime_seq_nr: &str,
    datetime_seq_nr: String,
    payload: Option<HashMap<String, AttributeValue>>
) -> Result<(), DdbError> {
//) -> impl Future<Output = Result<PutItemOutput, SdkError<PutItemError>>> {
    let account_nr_av = AttributeValue::S(account_nr.into());
    let datetime_seq_nr_av = AttributeValue::S(datetime_seq_nr.clone());

    let request = client
        .put_item()
        .table_name(table)
        .set_item(payload.clone())
        .item("account_nr", account_nr_av)
        .item("datetime_seq_nr", datetime_seq_nr_av);

    request.send().await?;

    // add a second field
    // let account_nr_av = AttributeValue::S(account_nr.to_owned()+"_00");
    // let datetime_seq_nr_av = AttributeValue::S(datetime_seq_nr.to_owned()+"-duplicate");

    // let request = client
    //     .put_item()
    //     .table_name(table)
    //     .set_item(payload)
    //     .item("account_nr", account_nr_av)
    //     .item("datetime_seq_nr", datetime_seq_nr_av);

    // request.send().await?;

    Ok(())
}


//fn get_waker(label: &str) -> fn() {
//    move | | println!("Finished {label}") 
//}


/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    // Extract some useful info from the request
    let num_request = event.payload.num_request;
    let multiple_account = event.payload.multiple_account;

  let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    println!("RegionProvider set to value: {:?}", region_provider);
    let config = aws_config::from_env().region(region_provider).load().await;
//    println!("The final config is: {:?}", config);
    let client = Client::new(&config);

    let resp = client.list_tables().send().await?;
                                                                                                                                   
    let mut msgs = Vec::new();
                                                                                                                                   
    let names = resp.table_names().unwrap_or_default();                                                                            
                                                                                                                                   
    for name in names {                                                                                                            
        msgs.push(format!(" Table:  {name}"));                                                                                                    
    }

    let mut to_await = Vec::new();                                                                                                                                                                                            
    let start = Instant::now();                                                                                                                                                                                               
                                                                                                                                                                                                                              
    for i in 0..num_request {                                                                                                                                                                                                          
        let dtsn = format!("2022-09-24 11:49 - {}", i);                                                                                                                                                                       
        let mut payload = HashMap::new();
        payload.insert("payload".to_owned(), AttributeValue::S(dtsn.clone()));
        // create future and poll it to start the process.
        let account_nr = if multiple_account { format!("{ACCOUNT_NR}_{i}") } else { ACCOUNT_NR.to_owned()};
        let ft = add_item(&client, "tx_data", account_nr, dtsn, Some(payload));
        // //let _ =  Pin::new(&mut ft).poll(&mut Context::from_waker(&get_waker(&payload));
        // let _ =  Pin::new(&mut ft).poll();
        // //let _ =  ft.poll();
        // to_await.push(ft);
        to_await.push(ft);
    }

    // await all prepared futures in parallel
    if let Err(err) = try_join_all(to_await).await {
        msgs.push(format!("Error observed {:?}", err));
    } else {
        msgs.push(format!("joined all {num_request} branches each having {NUM_TX_PER_REQUEST} TX successful"));
    };

    let duration = start.elapsed();

    msgs.push(format!("Total time elapsed is: {:?}", duration));


    // Prepare the response
    let resp = Response {
        req_id: event.context.request_id,
        msg: format!("Timings:\n\t {}.", msgs.join("\n\t")),
    };

    // Return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}




