#![allow(unused_imports)]

use async_ucx::ucp::*;
use std::io::Result;
use std::mem::{transmute,MaybeUninit};
use std::sync::atomic::*;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let local = tokio::task::LocalSet::new();
    if let Some(server_addr) = std::env::args().nth(1) {
        local.run_until(client(server_addr)).await?;
    } else {
        local.run_until(server()).await?;
    }
    Ok(())
}

// async fn client(server_addr: String) -> ! {
//     println!("client: connect to {:?}", server_addr);

//     let context = Context::new().unwrap();
//     let worker = context.create_worker().unwrap();
//     #[cfg(not(feature = "event"))]
//     tokio::task::spawn_local(worker.clone().polling());
//     #[cfg(feature = "event")]
//     tokio::task::spawn_local(worker.clone().event_poll());

//     let endpoint = worker
//         .connect_socket(server_addr.parse().unwrap())
//         .await
//         .unwrap();
//     endpoint.print_to_stderr();

//     let mut id = [MaybeUninit::uninit()];
//     endpoint.worker().tag_recv(100, &mut id).await.unwrap();
//     let tag = unsafe { id[0].assume_init() } as u64 + 200;
//     println!("client: got tag {:?}", tag);

//     let long_msg: Vec<u8> = (0..47008).map(|x| x as u8).collect();
//     loop {
//         endpoint.tag_send(tag, &long_msg).await.unwrap();
//         endpoint
//             .worker()
//             .tag_recv(tag, &mut [MaybeUninit::uninit()])
//             .await
//             .unwrap();
//     }
// }

// async fn server() -> ! {
//     println!("server");
//     let context = Context::new().unwrap();
//     let worker = context.create_worker().unwrap();
//     #[cfg(not(feature = "event"))]
//     tokio::task::spawn_local(worker.clone().polling());
//     #[cfg(feature = "event")]
//     tokio::task::spawn_local(worker.clone().event_poll());

//     let mut listener = worker
//         .create_listener("0.0.0.0:10000".parse().unwrap())
//         .unwrap();
//     tokio::task::spawn(async {
//         loop {
//             tokio::time::sleep(std::time::Duration::from_secs(1)).await;
//             let count = COUNT.swap(0, Ordering::SeqCst);
//             println!("{} IOPS", count);
//         }
//     });
//     println!("listening on {}", listener.socket_addr().unwrap());

//     for i in 0u8.. {
//         let conn = listener.next().await;
//         conn.remote_addr().unwrap();
//         let ep = worker.accept(conn).await.unwrap();
//         println!("accept {}", i);
//         ep.tag_send(100, &[i]).await.unwrap();
//         tokio::task::spawn_local(async move {
//             let tag = i as u64 + 200;
//             let mut buf = vec![MaybeUninit::uninit(); 50000];
//             loop {
//                 ep.worker().tag_recv(tag, &mut buf).await.unwrap();
//                 ep.tag_send(tag, &[0]).await.unwrap();
//                 COUNT.fetch_add(1, Ordering::SeqCst);
//             }
//         });
//     }
//     unreachable!()
// }

async fn client(server_addr: String) -> Result<()> {
    println!("client: connect to {:?}", server_addr);
    let context = Context::new().unwrap();
    let worker = context.create_worker().unwrap();
    println!("worker created");
    let endpoint = worker
        .connect_socket(server_addr.parse().unwrap())
        .await
        .unwrap();
    endpoint.print_to_stderr();
    #[cfg(not(feature = "event"))]
    tokio::task::spawn_local(worker.clone().polling());

    // register memory region
    let mut buf: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
    let mem = MemoryHandle::register(&context, &mut buf);
    endpoint
        .stream_send(&(buf.as_ptr() as u64).to_ne_bytes())
        .await
        .unwrap();
    endpoint.stream_send(mem.pack().as_ref()).await.unwrap();
    println!("send memory handle");

    endpoint
        .stream_recv(&mut [MaybeUninit::uninit(); 1])
        .await
        .unwrap();
    Ok(())
}

async fn server() -> Result<()> {
    println!("server");
    let context = Context::new().unwrap();
    let worker = context.create_worker().unwrap();
    let mut listener = worker
        .create_listener("0.0.0.0:10000".parse().unwrap())
        .unwrap();
    #[cfg(not(feature = "event"))]
    tokio::task::spawn_local(worker.clone().polling());
    println!("listening on {}", listener.socket_addr().unwrap());
    let connection = listener.next().await;
    let endpoint = worker.accept(connection).await.unwrap();
    println!("accept");
    endpoint.print_to_stderr();

    let mut vaddr_buf = [MaybeUninit::uninit(); 8];
    let len = endpoint.stream_recv(&mut vaddr_buf).await.unwrap();
    assert_eq!(len, 8);
    let vaddr = u64::from_ne_bytes(unsafe { transmute(vaddr_buf) });
    println!("recv: vaddr={:#x}", vaddr);

    let mut rkey_buf = [MaybeUninit::uninit(); 100];
    let len = endpoint.stream_recv(&mut rkey_buf).await.unwrap();
    println!("recv rkey: len={}", len);

    let rkey = RKey::unpack(&endpoint, unsafe { transmute(&rkey_buf[..len]) });
    let mut buf = vec![0; 0x1000];
    endpoint.get(&mut buf, vaddr, &rkey).await.unwrap();
    println!("get remote memory");

    let expected: Vec<u8> = (0..0x1000).map(|x| x as u8).collect();
    assert_eq!(&buf, &expected);

    endpoint.stream_send(&[0; 1]).await.unwrap();
    Ok(())
}

// static COUNT: AtomicUsize = AtomicUsize::new(0);