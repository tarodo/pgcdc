use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::error::PgcdcError;
use crate::lsn::{Lsn, LsnTracker};
use crate::postgres::guard::{check_reconnect, preflight_cold_start};
use crate::postgres::pgoutput::decode;
use crate::schema::RelationCache;
use crate::sink::Sink;
use crate::transaction::Assembler;

/// Строка подключения для репликационного соединения требует
/// `replication=database` — без него сервер откроет обычную сессию.
fn replication_url(base: &str) -> String {
    if base.contains('?') {
        format!("{base}&replication=database")
    } else {
        format!("{base}?replication=database")
    }
}

/// Можно ли подтвердить позицию, пришедшую в keepalive.
///
/// «Буфер пуст» само по себе НЕ достаточно (DECISIONS Q26a). Оно было достаточным,
/// пока запись, отметка durable и подтверждение происходили в одной итерации; с
/// групповым барьером появляется окно, где открытых транзакций нет, а принятые
/// sink'ом и не доведённые до носителя данные есть. Подтвердить позицию внутри
/// этого окна — значит подтвердить сверх durable и потерять данные при крахе.
fn may_advance_from_keepalive(assembler_empty: bool, processed: Lsn, durable: Lsn) -> bool {
    assembler_empty && processed <= durable
}

/// Состояние, которое переживает обрыв соединения.
///
/// Разделение здесь не косметическое. Позиции трекера **переносятся** через
/// реконнект: они монотонны, поэтому replay уже обработанных транзакций не
/// может сдвинуть их назад, а durable-позиция — это ровно то, с чем
/// `check_reconnect` сравнивает `confirmed_flush_lsn` слота. Обнулить трекер
/// значило бы уничтожить единственный вход этой проверки.
///
/// Кэш отношений и сборщик, наоборот, **сбрасываются**: кэш живёт в рамках
/// сессии репликации и после разрыва может описывать устаревшую схему
/// (DECISIONS Q19), а недособранная транзакция придёт заново целиком, потому
/// что её BEGIN был после `confirmed_flush_lsn`.
pub(crate) struct SessionState {
    tracker: LsnTracker,
    assembler: Assembler,
    cache: RelationCache,
}

impl SessionState {
    fn new(max_transaction_events: usize) -> Self {
        Self {
            tracker: LsnTracker::new(),
            assembler: Assembler::new(max_transaction_events),
            cache: RelationCache::new(),
        }
    }

    fn reset_for_reconnect(&mut self) {
        self.cache.clear();
        self.assembler.reset();
    }

    fn durable(&self) -> Lsn {
        self.tracker.durable()
    }
}

/// Чем закончилась одна сессия репликации.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionOutcome {
    /// Соединение оборвалось. Внешний цикл решает, переподключаться ли.
    Disconnected,
    /// Пришёл сигнал завершения и текущая группа доведена до барьера.
    ShutdownRequested,
}

/// Ставит флаг по SIGTERM или SIGINT. Флаг проверяется в начале каждого прохода
/// цикла; поскольку чтение и так ограничено по времени, задержка реакции не
/// превышает `ack_interval`. Это проще, чем городить select вокруг чтения, и
/// не трогает порядок операций, проверенный мутационно.
fn spawn_shutdown_listener() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));
    let f = flag.clone();
    tokio::spawn(async move {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "cannot install SIGTERM handler");
                    return;
                }
            };
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        f.store(true, Ordering::Relaxed);
    });
    flag
}

/// Удвоение с потолком. `saturating_mul` вместо `*` — чтобы удвоение у верха
/// диапазона не паниковало в debug-сборке.
fn next_backoff(current: Duration, max: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > max {
        max
    } else {
        doubled
    }
}

/// Есть ли уже durable-позиция, с которой можно сверять слот. На холодном
/// старте сравнивать не с чем — durable ещё ноль; сверка осмысленна только
/// со второго подключения и далее.
fn is_reconnect(durable: Lsn) -> bool {
    durable > Lsn(0)
}

/// Пауза перед следующей попыткой подключения. Обёрнута в тип, а не голый
/// `Duration`, живущий внутри бесконечного цикла `run()` с настоящими
/// `sleep`, — ради тестируемости: в таком виде мутация "убрать сброс" не
/// ловилась ни одним тестом (review Task 2, round 1, F2).
struct ReconnectBackoff {
    current: Duration,
    initial: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    fn new(initial: Duration, max: Duration) -> Self {
        Self {
            current: initial,
            initial,
            max,
        }
    }

    /// Продуктивная сессия сбрасывает паузу на начальную: без этого один
    /// долгий простой навсегда оставлял бы паузу на потолке, и следующий
    /// одиночный сбой через неделю ждал бы полминуты впустую. Что считать
    /// продуктивностью — решает вызывающий; передавать сюда нужно движение
    /// ПОДТВЕРЖДЁННОЙ позиции, а не принятой: keepalive-продвижение простаивающей
    /// публикации подтверждает WAL, ничего не читая, и было бы ошибочно
    /// признано непродуктивным (review Task 2, round 1, F1).
    ///
    /// Возвращает паузу, которую нужно выждать ПЕРЕД этой попыткой, и сама
    /// продвигается для следующего вызова.
    fn next_delay(&mut self, productive: bool) -> Duration {
        if productive {
            self.current = self.initial;
        }
        let delay = self.current;
        self.current = next_backoff(self.current, self.max);
        delay
    }
}

/// Доводит принятое sink'ом до барьера, отмечает durable, подтверждает
/// трекером и отправляет feedback серверу. Общий код для группового таймера
/// и для завершения по сигналу: без извлечения в отдельную функцию эти два
/// места разошлись бы, а мутационное покрытие, снятое против таймерной
/// ветки, не защищало бы вторую копию (см. бриф задачи 3).
async fn flush_and_acknowledge(
    sink: &mut Box<dyn Sink>,
    state: &mut SessionState,
    stream: &mut LogicalReplicationStream,
) -> Result<(), PgcdcError> {
    // Отметить durable имеет право только успешный барьер, а не приём записи.
    if let Some(durable) = sink.flush().await? {
        state.tracker.note_durable(durable);
        state.tracker.try_ack(durable)?;
        let acked = state.tracker.acked();

        // Отчитываемся позицией трекера (acked), а не тем, что вернул
        // барьер: сегодня они совпадают, но с реконнектом внутри
        // процесса (следующий этап) replay уже подтверждённой
        // транзакции может вернуть из flush позицию позади слота —
        // отправить её в feedback значило бы откатить сервер назад.
        // Подтверждаем acked, НЕ commit_lsn: commit_lsn указывает на
        // начало записи коммита, и рестарт перечитал бы ту же транзакцию.
        stream.shared_lsn_feedback.update_flushed_lsn(acked.0);
        stream.shared_lsn_feedback.update_applied_lsn(acked.0);

        // Обязательство Q25(в): без явного вызова подтверждение уходит
        // с задержкой 18–22 с по внутреннему расписанию крейта.
        stream
            .send_feedback()
            .await
            .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;

        debug!(lsn = %acked, "group_acknowledged");
    }
    Ok(())
}

pub async fn run(config: Config, mut sink: Box<dyn Sink>) -> Result<(), PgcdcError> {
    // Первым делом — до любого подключения и любого лога, где могла бы всплыть строка.
    config.database_url.validate()?;
    config.validate_reconnect_bounds()?;

    let mut state = SessionState::new(config.max_transaction_events);
    let mut backoff = ReconnectBackoff::new(
        Duration::from_millis(config.reconnect_initial_ms),
        Duration::from_millis(config.reconnect_max_ms),
    );
    let mut attempt: u32 = 0;

    // Флаг создаётся один раз ДО внешнего цикла и передаётся одной и той же
    // ссылкой в каждую сессию: если создавать его заново на каждом реконнекте,
    // после первого обрыва процесс перестал бы реагировать на сигнал.
    let shutdown = spawn_shutdown_listener();

    loop {
        let acked_before = state.tracker.acked();

        match stream_once(&config, &mut sink, &mut state, &shutdown).await {
            Ok(SessionOutcome::ShutdownRequested) => return Ok(()),
            Ok(SessionOutcome::Disconnected) => {}
            // Восстановимые ошибки ведут в реконнект, фатальные — наружу.
            // Классификация живёт в типе (`is_fatal`), а не в разборе текста.
            Err(e) if !e.is_fatal() => {
                warn!(error = %e, error_kind = e.kind(), "postgres_connection_lost");
            }
            Err(e) => return Err(e),
        }

        // Признак продуктивности — сдвинулась ПОДТВЕРЖДЁННАЯ позиция, а не
        // принятая: и групповое подтверждение, и keepalive-продвижение на
        // простаивающей публикации двигают acked, а вот received трогает
        // только приход кадра данных (review Task 2, round 1, F1).
        let productive = state.tracker.acked() > acked_before;
        if productive {
            attempt = 0;
        }
        attempt += 1;
        let delay = backoff.next_delay(productive);
        warn!(
            retry = attempt,
            backoff_ms = delay.as_millis() as u64,
            "reconnecting"
        );
        tokio::time::sleep(delay).await;

        // Кэш и сборщик сбрасываются, позиции переносятся.
        state.reset_for_reconnect();
    }
}

/// Одна сессия репликации: preflight, подключение, цикл. Возвращается при
/// обрыве соединения или при штатном завершении.
async fn stream_once(
    config: &Config,
    sink: &mut Box<dyn Sink>,
    state: &mut SessionState,
    shutdown: &Arc<AtomicBool>,
) -> Result<SessionOutcome, PgcdcError> {
    // Захватываем ДО preflight, а не проверяем `state.durable()` заново
    // позже: решение "это реконнект" принимается на входе в функцию и не
    // должно незаметно подстроиться под то, что случится дальше внутри неё
    // (review Task 2, round 1, F7).
    let reconnecting = is_reconnect(state.durable());

    // Обязательство Q25(а): guard ДО start(), потому что start() безусловно
    // зовёт ensure_replication_slot() и при отсутствующем слоте молча создаст
    // новый на текущей позиции WAL, потеряв всё закоммиченное раньше.
    let info_slot = preflight_cold_start(config.database_url.expose(), &config.slot).await?;
    info!(
        slot = %config.slot,
        restart_lsn = ?info_slot.restart_lsn.map(|l| l.to_string()),
        confirmed_flush_lsn = ?info_slot.confirmed_flush_lsn.map(|l| l.to_string()),
        "slot_preflight_ok"
    );

    // Проверка реконнекта: на холодном старте сравнивать не с чем, durable ещё
    // ноль. На повторном подключении позиция в памяти есть, и сверка ничего не
    // стоит. Слот ВПЕРЁД нашей durable-точки означает, что кто-то подтвердил
    // WAL, который мы не довели до sink, — падаем. Слот ПОЗАДИ — ожидаемый
    // исход обрыва: последний feedback мог не дойти. Пишем предупреждение и
    // продолжаем, промежуток перечитается дубликатами (DECISIONS R11 этапа 0).
    if reconnecting {
        if let Some(warning) = check_reconnect(&config.slot, &info_slot, state.durable())? {
            warn!("{warning}");
        }
    }

    let stream_config = ReplicationStreamConfig::new(
        config.slot.clone(),
        config.publication.clone(),
        1,
        StreamingMode::Off,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    // Наш декодер понимает только текстовые значения (pgoutput.rs) и не подписан
    // на pg_logical_emit_message — оба уже выключены значениями по умолчанию
    // крейта, но фиксируем это явно здесь, а не полагаемся молча на них.
    .with_binary(false)
    .with_messages(false);

    let url = replication_url(config.database_url.expose());
    let mut stream = LogicalReplicationStream::new(&url, stream_config)
        .await
        .map_err(|e| PgcdcError::Connection(format!("open replication stream: {e}")))?;

    // start_lsn = None означает 0/0: сервер возьмёт confirmed_flush_lsn слота.
    // Слот — единственный источник истины (DECISIONS Q4, Q19).
    stream
        .start(None)
        .await
        .map_err(|e| PgcdcError::Connection(format!("start replication: {e}")))?;
    info!(slot = %config.slot, publication = %config.publication, "replication_started");

    if reconnecting {
        // Только теперь: поток реально открыт и запущен сервером. Залогировать
        // это сразу после проверки слота означало бы заявить восстановление
        // раньше, чем сервер его подтвердил, — на нестабильном сервере лог
        // обещал бы восстановление, за которым тут же следует новый обрыв
        // (review Task 2, round 1, F7).
        info!(slot = %config.slot, "postgres_connection_restored");
    }

    if sink.durability() == crate::sink::Durability::BestEffort {
        warn!(
            "sink is best-effort, not durable: acknowledged positions may outlive unwritten output"
        );
    }

    let cancel = CancellationToken::new();

    // Барьер на каждой транзакции означает fsync на каждую транзакцию —
    // потолок порядка сотни транзакций в секунду. Группируем по таймеру, не
    // трогая порядок операций внутри одного прохода: sink, потом барьер,
    // потом durable, только потом ack, только потом feedback.
    let ack_interval = Duration::from_millis(config.ack_interval_ms);
    let mut last_flush = tokio::time::Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Довести принятое до барьера и подтвердить, прежде чем выйти.
            // Выйти раньше значило бы потерять уже принятые транзакции.
            flush_and_acknowledge(sink, state, &mut stream).await?;
            info!("shutdown_requested");
            return Ok(SessionOutcome::ShutdownRequested);
        }

        // Ограниченное чтение здесь безопасно, потому что прод работает на
        // многопоточном рантайме: транспорт выбирает Inline-драйвер по
        // флейвору рантайма, и его буфер чтения живёт на соединении, а не в
        // сброшенной future, — отменённое чтение не теряет уже прочитанный,
        // но не отданный кадр (проверено против исходника крейта,
        // docs/spike-findings.md, «Обходной путь 6»). Поведение драйвера
        // однопоточного рантайма при таком же падении future НЕ установлено;
        // интеграционные тесты несут `flavor = "multi_thread"` не потому, что
        // там доказана потеря кадров, а по общему принципу: тест обязан
        // гонять тот же драйвер, что и прод (задача 1; формулировка уточнена
        // в задаче 4, review round 1, F2).
        //
        // Разрешён ТОЛЬКО next_raw_event: остальные пять API ведут в
        // recover_connection, который рестартует с last_received_lsn —
        // принятой, а не durable позиции (Q25(б)).
        let read = tokio::time::timeout(ack_interval, stream.next_raw_event(&cancel)).await;

        match read {
            Ok(Ok(raw)) => {
                state.tracker.note_received(Lsn(raw.wal_end.0));

                let msg = decode(&raw.data)?;
                if let Some(tx) =
                    state
                        .assembler
                        .handle(msg, Lsn(raw.wal_start.0), &mut state.cache)?
                {
                    let changes = tx.changes.len();
                    let end_lsn = tx.end_lsn;

                    // Порядок нерушим: сначала sink, потом барьер, потом durable, только потом ack.
                    sink.write_transaction(&tx).await?;
                    state.tracker.note_processed(end_lsn);
                    debug!(xid = tx.xid, changes, lsn = %end_lsn, "transaction_accepted");
                }
            }
            Ok(Err(e)) => {
                warn!(error = %e, "postgres_connection_lost");
                return Ok(SessionOutcome::Disconnected);
            }
            // Тик: читать было нечего. Не ошибка — повод дойти до барьера.
            Err(_elapsed) => {}
        }

        if last_flush.elapsed() >= ack_interval {
            last_flush = tokio::time::Instant::now();
            flush_and_acknowledge(sink, state, &mut stream).await?;
        }

        // Продвижение по keepalive: если мы ничего не должны sink'у, вся позиция,
        // которую сервер уже отдал, вакуумно durable — в ней не было ни одной
        // строки нашей публикации. Отмечаем это явно, а не ослабляем try_ack
        // (DECISIONS Q26b).
        let server_lsn = Lsn(stream.current_lsn());
        if may_advance_from_keepalive(
            state.assembler.is_empty(),
            state.tracker.processed(),
            state.tracker.durable(),
        ) && server_lsn > state.tracker.acked()
        {
            state.tracker.note_durable(server_lsn);
            state.tracker.try_ack(server_lsn)?;
            stream.shared_lsn_feedback.update_flushed_lsn(server_lsn.0);
            stream.shared_lsn_feedback.update_applied_lsn(server_lsn.0);
            stream
                .send_feedback()
                .await
                .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;
            debug!(lsn = %server_lsn, "advanced_from_keepalive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_reconnect_is_false_on_a_cold_start() {
        assert!(!is_reconnect(Lsn(0)));
    }

    #[test]
    fn is_reconnect_is_true_once_something_is_durable() {
        assert!(is_reconnect(Lsn(0x1000)));
    }

    #[test]
    fn backoff_resets_to_initial_after_a_productive_session() {
        let mut b = ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        // Взбираемся к потолку серией непродуктивных попыток.
        for _ in 0..10 {
            b.next_delay(false);
        }
        assert_eq!(
            b.next_delay(true),
            Duration::from_millis(100),
            "продуктивная сессия обязана сбросить паузу на начальную"
        );
    }

    #[test]
    fn backoff_doubles_until_it_reaches_the_ceiling() {
        let max = Duration::from_millis(1000);
        assert_eq!(
            next_backoff(Duration::from_millis(100), max),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(400), max),
            Duration::from_millis(800)
        );
        assert_eq!(
            next_backoff(Duration::from_millis(800), max),
            max,
            "упирается в потолок"
        );
        assert_eq!(next_backoff(max, max), max, "и остаётся на нём");
    }

    #[test]
    fn backoff_cannot_overflow() {
        // Удвоение у самого верха диапазона не должно паниковать в debug-сборке.
        let huge = Duration::from_millis(u64::MAX / 2 + 1);
        assert_eq!(
            next_backoff(huge, Duration::from_millis(1000)),
            Duration::from_millis(1000)
        );
    }

    #[test]
    fn keepalive_advance_requires_an_empty_buffer() {
        // Открытая транзакция означает, что часть WAL мы ещё должны sink'у.
        assert!(!may_advance_from_keepalive(false, Lsn(0x1000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_requires_processed_to_have_caught_up() {
        // Буфер пуст, но транзакция принята sink'ом и ещё не доведена барьером.
        // Подтвердить позицию из keepalive здесь — значит подтвердить сверх durable.
        assert!(!may_advance_from_keepalive(true, Lsn(0x2000), Lsn(0x1000)));
    }

    #[test]
    fn keepalive_advance_is_allowed_when_nothing_is_owed() {
        assert!(may_advance_from_keepalive(true, Lsn(0x1000), Lsn(0x1000)));
    }

    #[test]
    fn reconnect_resets_the_cache_and_the_assembler() {
        // Кэш живёт в рамках сессии: сервер перешлёт RELATION в новой сессии,
        // а старое описание могло устареть, пока нас не было. Недособранная
        // транзакция придёт заново целиком — её BEGIN был после confirmed_flush_lsn.
        let mut s = SessionState::new(1000);
        s.cache.put(crate::schema::Relation {
            id: 1,
            namespace: "public".into(),
            name: "users".into(),
            replica_identity: b'f',
            columns: vec![],
        });
        s.assembler
            .handle(
                crate::postgres::pgoutput::PgOutputMessage::Begin {
                    final_lsn: 0x1000,
                    commit_timestamp: 0,
                    xid: 7,
                },
                Lsn(0x100),
                &mut s.cache,
            )
            .unwrap();
        assert_eq!(s.cache.len(), 1);
        assert!(!s.assembler.is_empty());

        s.reset_for_reconnect();

        assert_eq!(s.cache.len(), 0, "кэш сбрасывается целиком");
        assert!(
            s.assembler.is_empty(),
            "недособранная транзакция выбрасывается"
        );
    }

    #[test]
    fn reconnect_carries_the_tracker_positions_forward() {
        // Позиции НЕ сбрасываются. Обнулить трекер значило бы потерять
        // durable-позицию, с которой check_reconnect сравнивает слот, — и
        // заодно открыть гейт keepalive в момент, когда replay ещё не догнал.
        let mut s = SessionState::new(1000);
        s.tracker.note_received(Lsn(0x3000));
        s.tracker.note_processed(Lsn(0x2000));
        s.tracker.note_durable(Lsn(0x2000));
        s.tracker.try_ack(Lsn(0x2000)).unwrap();

        s.reset_for_reconnect();

        assert_eq!(s.durable(), Lsn(0x2000), "durable переносится");
        assert_eq!(s.tracker.acked(), Lsn(0x2000), "подтверждённая переносится");
        assert_eq!(s.tracker.processed(), Lsn(0x2000), "processed переносится");
    }

    #[test]
    fn replayed_transactions_cannot_move_positions_backwards() {
        // После реконнекта сервер отдаёт заново всё после confirmed_flush_lsn.
        // Позиции монотонны, поэтому повторная обработка их не откатывает,
        // а гейт keepalive остаётся закрытым, пока replay не догонит processed.
        let mut s = SessionState::new(1000);
        s.tracker.note_processed(Lsn(0x2000));
        s.tracker.note_durable(Lsn(0x2000));
        s.reset_for_reconnect();
        s.tracker.note_processed(Lsn(0x1000));
        assert_eq!(s.tracker.processed(), Lsn(0x2000));
    }
}
