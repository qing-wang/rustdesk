// TaipeiBridge 更新交握。
//
// TaipeiBridge 以「整個置換程式目錄」的方式更新，置換前它的 Agent 會透過具名管道
// 問正在執行的程式「現在方不方便更新」。沒有交握的話它只能硬殺 —— 對遠端桌面程式
// 來說，硬殺等於把正在遠端操作這台機器的人直接踢掉。所以這裡在主行程開一個小的
// listener，有進行中的被控連線時回報忙碌，閒置時才放行並自行結束。
//
// 協定（四行 JSON，非 .NET 專屬）：
//   agent -> app  {"cmd":"prepareUpdate","timeoutSeconds":N}
//   app  -> agent {"state":"busy","detail":"..."}   // 可重複
//   app  -> agent {"state":"ready"}                  // 之後自行結束
//   app  -> agent {"state":"refused","reason":"..."}
//   agent -> app  {"cmd":"ping"} -> {"state":"alive"}
//
// 只在 Windows 的主 UI 行程啟動（見 core_main.rs 的空參數分支），因為被控連線的
// 登錄表 AUTHED_CONNS 活在這個行程，且 maxNumberOfServerInstances=1 要求只有主行程
// 開這個管道。管道名稱必須與 release.json 的 "shutdown.pipe" 對齊（Agent 會補上
// \\.\pipe\ 前綴），所以這裡不用 Config::ipc_path（那會帶 APP_NAME 前綴對不上）。
#![cfg(windows)]

use hbb_common::{
    bytes::Bytes,
    bytes_codec::BytesCodec,
    futures::StreamExt,
    futures_util::sink::SinkExt,
    log,
    tokio::{
        self,
        time::{sleep, Duration, Instant},
    },
    tokio_util::codec::Framed,
    ResultType,
};
use parity_tokio_ipc::{Endpoint, SecurityAttributes};

// 必須與 release.json 的 "shutdown.pipe"（taipeibridge-rustdeskq-client）一致。
const PIPE_PATH: &str = r"\\.\pipe\taipeibridge-rustdeskq-client";
// 忙碌時每隔幾秒回報一次，並重新檢查是否已閒置。
const BUSY_POLL_SECS: u64 = 3;

type Pipe = Framed<parity_tokio_ipc::Connection, BytesCodec>;

/// 在主行程以獨立 OS 執行緒＋自有 current-thread runtime 啟動 listener。
pub fn start() {
    std::thread::spawn(|| {
        if let Err(e) = run() {
            log::error!("update_gate listener exited: {:?}", e);
        }
    });
}

#[tokio::main(flavor = "current_thread")]
async fn run() -> ResultType<()> {
    let mut endpoint = Endpoint::new(PIPE_PATH.to_string());
    // Agent 與本程式同一使用者執行；allow_everyone_create 可避免 ACL 造成連不上。
    if let Ok(attr) = SecurityAttributes::allow_everyone_create() {
        endpoint.set_security_attributes(attr);
    }
    let mut incoming = endpoint.incoming()?;
    log::info!("update_gate listening on {}", PIPE_PATH);
    while let Some(conn) = incoming.next().await {
        match conn {
            Ok(stream) => {
                tokio::spawn(async move {
                    let mut pipe = Framed::new(stream, BytesCodec::new());
                    if let Err(e) = handle(&mut pipe).await {
                        log::trace!("update_gate connection ended: {:?}", e);
                    }
                });
            }
            Err(e) => log::error!("update_gate accept error: {:?}", e),
        }
    }
    Ok(())
}

async fn handle(pipe: &mut Pipe) -> ResultType<()> {
    let first = match pipe.next().await {
        Some(Ok(b)) => b,
        _ => return Ok(()),
    };
    let msg: serde_json::Value = serde_json::from_slice(&first).unwrap_or(serde_json::Value::Null);
    match msg.get("cmd").and_then(|c| c.as_str()) {
        Some("ping") => {
            reply(pipe, &serde_json::json!({ "state": "alive" })).await?;
        }
        Some("prepareUpdate") => {
            let timeout_secs = msg
                .get("timeoutSeconds")
                .and_then(|t| t.as_u64())
                .unwrap_or(300);
            let deadline = Instant::now() + Duration::from_secs(timeout_secs);
            loop {
                let n = active_sessions();
                if n == 0 {
                    reply(pipe, &serde_json::json!({ "state": "ready" })).await?;
                    // 確保對方讀到 ready 再結束整個程式，讓 Agent 置換目錄。
                    sleep(Duration::from_millis(300)).await;
                    log::info!("update_gate: idle, replied ready, quitting for update");
                    crate::platform::quit_gui();
                    return Ok(());
                }
                reply(
                    pipe,
                    &serde_json::json!({
                        "state": "busy",
                        "detail": format!("有 {} 條連線進行中", n),
                    }),
                )
                .await?;
                if Instant::now() >= deadline {
                    // 逾時仍忙：不回 ready，讓 Agent 自行延後（Deferred）。
                    log::info!("update_gate: still busy at deadline ({} conns), deferring", n);
                    return Ok(());
                }
                sleep(Duration::from_secs(BUSY_POLL_SECS)).await;
            }
        }
        _ => {
            reply(
                pipe,
                &serde_json::json!({ "state": "refused", "reason": "unknown command" }),
            )
            .await?;
        }
    }
    Ok(())
}

async fn reply(pipe: &mut Pipe, v: &serde_json::Value) -> ResultType<()> {
    pipe.send(Bytes::from(serde_json::to_vec(v)?)).await?;
    Ok(())
}

/// 這台機器此刻進行中的被控連線數（遠端控制／檔案傳輸／終端／連接埠轉送／看鏡頭）。
/// 任何一條都代表有人正在使用這台機器，更新應延後。
fn active_sessions() -> usize {
    crate::server::AUTHED_CONNS
        .lock()
        .map(|c| c.len())
        .unwrap_or(0)
}
