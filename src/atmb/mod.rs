use color_eyre::eyre::{bail, eyre};
use futures::StreamExt;
use log::info;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use tokio::time::{sleep, Duration};
use crate::atmb::model::Mailbox;
use crate::atmb::page::{CountryPage, LocationDetailPage, StatePage};
use crate::checkpoint::Checkpoint;
use crate::utils::retry_wrapper;

/// 每次向 ATMB 发出详情页请求后的等待时长（毫秒）。
/// 调低此值会加快速度但更容易触发速率限制；调高则更安全。
const DETAIL_PAGE_DELAY_MS: u64 = 1500;

mod page;
pub mod model;

const BASE_URL: &str = "https://www.anytimemailbox.com";
const US_HOME_PAGE_URL: &str = "/l/usa";

static USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
];

/// 从预设池中随机选取一个浏览器 User-Agent。
fn get_random_user_agent() -> &'static str {
    use rand::Rng;
    let idx = rand::thread_rng().gen_range(0..USER_AGENTS.len());
    USER_AGENTS[idx]
}

/// 返回 5000–12000 ms 之间均匀分布的随机延时（毫秒）。
fn get_random_delay() -> u64 {
    use rand::Rng;
    rand::thread_rng().gen_range(5_000u64..=12_000u64)
}


/// HTTP client for obtaining information from ATMB
struct ATMBClient {
    client: Client,
}

impl Clone for ATMBClient {
    fn clone(&self) -> Self {
        Self {
            client: Client::builder()
                .default_headers(Self::default_headers())
                .build()
                .unwrap(),
        }
    }
}

impl ATMBClient {
    fn new() -> color_eyre::Result<Self> {
        Ok(
            Self {
                client: Client::builder()
                    .default_headers(Self::default_headers())
                    .build()?,
            }
        )
    }

    fn default_headers() -> HeaderMap {
        let mut map = HeaderMap::new();
        map.insert(USER_AGENT, HeaderValue::from_static(get_random_user_agent()));
        map
    }

    /// get the content of a page
    ///
    /// * `url_path` - the path of the page, can be either a full URL or a relative path
    async fn fetch_page(&self, url_path: &str) -> color_eyre::Result<String> {
        let url = if url_path.starts_with("http") {
            url_path
        } else {
            &format!("{}{}", BASE_URL, url_path)
        };
        Ok(
            retry_wrapper(3, || async {
                self.client
                    .get(url)
                    .send()
                    .await?
                    .text()
                    .await
            }).await?
        )
    }
}

pub struct ATMBCrawl {
    client: ATMBClient,
}

impl ATMBCrawl {
    pub fn new() -> color_eyre::Result<Self> {
        Ok(Self {
            client: ATMBClient::new()?,
        })
    }

    pub async fn get_available_states(&self) -> color_eyre::Result<Vec<String>> {
        // 获取国家页面
        let country_html = self.client.fetch_page(US_HOME_PAGE_URL).await?;
        let country_page = CountryPage::parse_html(&country_html)?;
        
        // 提取所有州的名称
        let states = country_page.states.iter()
            .map(|state| state.name().to_string())
            .collect();
        
        Ok(states)
    }
    
    pub async fn fetch_selected_states(&self, selected_states: &[String], checkpoint: &mut Checkpoint) -> color_eyre::Result<Vec<Mailbox>> {
        // 获取国家页面
        let country_html = self.client.fetch_page(US_HOME_PAGE_URL).await?;
        let country_page = CountryPage::parse_html(&country_html)?;
        
        // 筛选出选中的州
        let filtered_states = country_page.states.iter()
            .filter(|state| selected_states.contains(&state.name().to_string()))
            .collect::<Vec<_>>();
        
        if filtered_states.is_empty() {
            bail!("No states found with the provided names");
        }
        
        // 创建一个新的CountryPage，只包含选中的州
        let filtered_country_page = CountryPage {
            states: filtered_states.into_iter().cloned().collect(),
        };
        
        // 获取选中州的页面
        let state_pages = self.fetch_state_pages(&filtered_country_page).await?;
        let total_num = state_pages.iter().map(|sp| sp.len()).sum::<usize>();
        
        let mailboxes = state_pages.into_iter()
            .filter_map(|sp| match sp.to_mailboxes() {
                Ok(mailboxes) => Some(mailboxes),
                Err(e) => {
                    log::error!("cannot convert state page to mailboxes: {:?}", e);
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        if mailboxes.len() != total_num {
            bail!("Some mailboxes cannot be fetched");
        }

        // 访问每个邮箱的详细页面以获取地址行2（串行 + 断点续传）
        self.update_street2_for_mailbox(mailboxes, checkpoint).await
    }
    
    pub async fn fetch(&self, checkpoint: &mut Checkpoint) -> color_eyre::Result<Vec<Mailbox>> {
        // we're only interested in US, so hardcode here.
        let country_html = self.client.fetch_page(US_HOME_PAGE_URL).await?;
        let country_page = CountryPage::parse_html(&country_html)?;

        let state_pages = self.fetch_state_pages(&country_page).await?;
        let total_num = state_pages.iter().map(|sp| sp.len()).sum::<usize>();

        let mailboxes = state_pages.into_iter()
            .filter_map(|sp| match sp.to_mailboxes() {
                Ok(mailboxes) => Some(mailboxes),
                Err(e) => {
                    log::error!("cannot convert state page to mailboxes: {:?}", e);
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        if mailboxes.len() != total_num {
            bail!("Some mailboxes cannot be fetched");
        }

        // 逐条抓取 detail 页面（串行 + 断点续传）
        self.update_street2_for_mailbox(mailboxes, checkpoint).await
    }

    /// 逐条（串行）抓取每个 mailbox 的 detail page，支持断点续传。
    ///
    /// * 每次请求后等待 `DETAIL_PAGE_DELAY_MS` 毫秒，大幅降低被 ATMB 速率限制的概率。
    /// * 若 checkpoint 中已有该 link 的记录，直接复用，无需网络请求。
    /// * 每成功抓取一条，立即原子写入 checkpoint 文件，Ctrl+C 中断后可从断点继续。
    async fn update_street2_for_mailbox(
        &self,
        mailboxes: Vec<Mailbox>,
        checkpoint: &mut Checkpoint,
    ) -> color_eyre::Result<Vec<Mailbox>> {
        let total = mailboxes.len();
        let resumed = checkpoint.completed_count();
        if resumed > 0 {
            info!(
                "Checkpoint loaded: {} already done, up to {} remaining.",
                resumed,
                total.saturating_sub(resumed)
            );
        }

        let mut results: Vec<Mailbox> = Vec::with_capacity(total);

        for (idx, mut mailbox) in mailboxes.into_iter().enumerate() {
            let pos = format!("[{}/{}]", idx + 1, total);

            // ── 断点续传：若已在 checkpoint 中则跳过网络请求 ──
            if let Some(cached_street) = checkpoint.get_completed(&mailbox.link) {
                info!("{} [resumed] {} — using checkpoint data", pos, mailbox.name);
                mailbox.address.line1 = cached_street.to_string();
                results.push(mailbox);
                continue;
            }

            info!("{} fetching detail page of [{}]...", pos, mailbox.name);

            // ── 带指数退避重试的请求（最多 5 次）──
            let fetch_result = retry_wrapper(5, || async {
                self.fetch_location_detail_page(&mailbox.link).await
            }).await;

            match fetch_result {
                Ok(detail_page) => {
                    let street = detail_page.street();
                    // 原子写入 checkpoint，即使之后 Ctrl+C 也不丢失此条
                    if let Err(e) = checkpoint.save_one(&mailbox.link, &street) {
                        log::warn!("{} failed to save checkpoint entry: {:?}", pos, e);
                    }
                    mailbox.address.line1 = street;
                    results.push(mailbox);
                }
                Err(err) => {
                    log::warn!(
                        "{} skipped [{}] after all retries failed: {:?}",
                        pos, mailbox.name, err
                    );
                    // 跳过此条，继续处理下一条，不让单条失败终止整个任务
                }
            }

            // ── 速率限制：每条请求后固定等待，防止 IP 被封 ──
            sleep(Duration::from_millis(DETAIL_PAGE_DELAY_MS)).await;
        }

        Ok(results)
    }

    async fn fetch_state_pages(&self, country_page: &CountryPage<'_>) -> color_eyre::Result<Vec<StatePage>> {
        let total_states = country_page.states.len();
        let state_pages: Vec<color_eyre::Result<StatePage>> = futures::stream::iter(&country_page.states).enumerate().map(|(idx, state_html_info)| {
            info!("[{}/{total_states}] fetching [{}] state page...", idx + 1, state_html_info.name());
            async move {
                // 错峰延时：两两并发，同一批内的第二个请求延迟 500ms，错开发出
                let stagger_ms = ((idx % 2) as u64) * 500;
                if stagger_ms > 0 {
                    sleep(Duration::from_millis(stagger_ms)).await;
                }
                let state_html = self.client.fetch_page(state_html_info.url()).await?;
                Ok(StatePage::parse_html(&state_html)?)
            }
        })
            // 州列表只有几十个，保持 2 并发即可；每批内有 500ms 错峰
            .buffer_unordered(2)
            .collect()
            .await;

        if state_pages.iter().filter_map(|state_page| match state_page {
            Err(e) => {
                log::error!("cannot fetch state: {:?}", e);
                Some(())
            }
            _ => None
        })
            .count() != 0 {
            bail!("Some states cannot be fetched");
        }
        Ok(state_pages.into_iter().map(|state_page| state_page.unwrap()).collect())
    }

    async fn fetch_location_detail_page(&self, mailbox_link: &str) -> color_eyre::Result<LocationDetailPage> {
        let html = self.client.fetch_page(mailbox_link).await?;
        Ok(LocationDetailPage::parse_html(&html)?)
    }
}