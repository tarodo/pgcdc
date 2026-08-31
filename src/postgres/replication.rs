use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationError, ReplicationStreamConfig,
    RetryConfig, StreamingMode,
};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::error::PgcdcError;
use crate::lsn::{Lsn, LsnTracker};
use crate::metrics::Metrics;
use crate::postgres::guard::{check_reconnect, preflight_slot};
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
///
/// Датчик буфера (`transaction_buffer_size`) обнуляется здесь же, отдельным
/// вызовом (F1, review Task 2, round 1): его единственный обычный сайт записи
/// живёт в приёмной ветке `stream_once` и срабатывает только на кадре
/// данных, а этот сброс происходит на обрыве соединения без единого нового
/// кадра. Не обнулить его значило бы держать последнее ненулевое значение
/// сколь угодно долго на простаивающей после обрыва публикации — гейджу, в
/// отличие от подтверждённой позиции, разрешено иметь второй сайт записи
/// именно потому, что он обязан уметь падать вне общего хвоста
/// `acknowledge_durable`.
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

    fn reset_for_reconnect(&mut self, metrics: &Metrics) {
        self.cache.clear();
        self.assembler.reset();
        metrics.set_transaction_buffer_size(0);
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

/// Ставит флаг по SIGTERM или SIGINT. Флаг читается в ТРЁХ местах: внутри
/// сессии (`stream_once`, на каждом обороте её цикла), на входе в каждый
/// проход внешнего цикла реконнекта (`run`) и внутри нарезанной паузы
/// бэкоффа между попытками — три, а не два, как утверждала более ранняя
/// версия этого комментария.
///
/// Величиной `SHUTDOWN_POLL_INTERVAL` ограничены только ДВА из них — чтение
/// внутри сессии и нарезка паузы бэкоффа, — не по счёту, а именно эти два:
/// не `ack_interval_ms` (тот управляет только расписанием барьера) и не
/// длиной самой паузы. Внутри сессии этой величиной ограничено само
/// чтение; нарезка бэкоффа проверяет флаг перед каждым куском той же
/// длины. Проверка на входе в проход внешнего цикла в этот список НЕ
/// входит: её период — целый оборот (сессия плюс пауза реконнекта), а не
/// `SHUTDOWN_POLL_INTERVAL`. Она не лишняя: нарезка проверяет флаг ПЕРЕД
/// каждым куском и ни разу ПОСЛЕ последнего, так что сигнал, попавший
/// именно в последний кусок, доходит только через эту третью,
/// неограниченную проверку на следующем обороте (подробности и цена такой
/// задержки — у самой проверки, `run`).
///
/// Граница держится только внутри этих мест, а не в промежутке между
/// чтением флага на входе во внешний цикл и первым чтением флага внутри
/// сессии: там лежит preflight, установка соединения и запуск репликации
/// (`stream_once` до входа в свой цикл) — ни один из этих шагов не смотрит
/// на флаг и не ограничен по времени. Против отказанного порта это не
/// стоит ничего: TCP отвечает отказом немедленно. Против адреса, который
/// не отвечает вовсе (чёрная дыра, файрвол, тянущий пакеты), сигнал может
/// остаться незамеченным десятки секунд — на длительность таймаутов
/// установления соединения, а не на `SHUTDOWN_POLL_INTERVAL`. Это проще,
/// чем городить select вокруг чтения, и не трогает порядок операций,
/// проверенный мутационно.
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

/// SQLSTATE гонки «слот ещё занят нашей же прошлой сессией»
/// (`ERRCODE_OBJECT_IN_USE`, `ReplicationSlotAcquire` в `slot.c` PostgreSQL).
const SLOT_BUSY_SQLSTATE: &str = "55006";

/// Достаёт SQLSTATE из строки ошибки `pg_walstream`, если он там есть.
///
/// Форматирование ответа сервера в этом крейте
/// (`connection/native/error.rs::PgErrorFields::Display`) кладёт код
/// состояния в ту же строку, что и текст сообщения:
/// `"{severity}: {message} (SQLSTATE {code})"`. Оба живых прогона против
/// реального Postgres это подтвердили дословно: `SQLSTATE 55000` на
/// инвалидированном слоте, `SQLSTATE 22023` на чужом output-плагине (round
/// after task 4). Код состояния — пятизначный идентификатор, который
/// протокол PostgreSQL никогда не переводит; `message` рядом с ним
/// переводится, если у сервера локализован `lc_messages`.
fn extract_sqlstate(message: &str) -> Option<&str> {
    const MARKER: &str = "(SQLSTATE ";
    let start = message.find(MARKER)? + MARKER.len();
    let rest = message.get(start..)?;
    let end = rest.find(')')?;
    let code = &rest[..end];
    (code.len() == 5 && code.bytes().all(|b| b.is_ascii_alphanumeric())).then_some(code)
}

/// Классифицирует отказ `stream.start()` (C1, review round after task 4).
///
/// До этой функции ЛЮБАЯ ошибка `START_REPLICATION` заворачивалась в
/// `PgcdcError::Connection` — восстановимый вариант — и процесс уходил в
/// вечный реконнект с потолком бэкоффа даже тогда, когда слот инвалидирован
/// (`SQLSTATE 55000`) или несёт чужой output-плагин (`SQLSTATE 22023`):
/// сервер ОТВЕТИЛ и явно отказал, а не бросил связь. Повторный
/// `START_REPLICATION` с теми же параметрами получит тот же отказ и через
/// час — ретраить его значит не восстанавливаться, а прятать необратимую
/// потерю доступа к WAL за видимостью работающего процесса (инвариант 3,
/// DECISIONS §1).
///
/// Различение опирается на `pg_walstream::ReplicationError::is_transient()`:
/// разрыв сокета или временная неполадка транспорта (`Io`/
/// `TransientConnection`/`Timeout`/`ReplicationConnection`/`Backend`)
/// остаётся восстановимой, а `Protocol` — которым крейт заворачивает и явный
/// отказ сервера на `START_REPLICATION`, и низкоуровневую ошибку разбора
/// самого проволочного формата (например, недопустимую длину сообщения,
/// `connection/native/copy.rs`) — фатален по умолчанию. Для второго случая
/// (порча самого протокола, а не отказ, адресованный слоту) вердикт
/// «фатально» верен так же, как и для первого — небезопасно молча ретраить
/// поток, чьё кодирование уже разошлось с ожидаемым, — но имя варианта,
/// `PgcdcError::SlotUnusable`, вводит в заблуждение: эта ветка шире своего
/// названия и ловит любой `Protocol`, а не только отказ, который сервер
/// адресовал именно слоту.
///
/// Единственное исключение — гонка «слот ещё занят нашей же прошлой
/// сессией» (`SQLSTATE 55006` = `ERRCODE_OBJECT_IN_USE`,
/// `ReplicationSlotAcquire` в `slot.c` PostgreSQL): сервер тоже отвечает, но
/// отказ здесь не про сам слот, а про то, что предыдущий walsender ещё не
/// успел его отпустить — наш же реконнект мог прийти раньше, чем сервер
/// дочистил прошлую сессию (DECISIONS Q19: каждый реконнект — новое
/// соединение и новый `START_REPLICATION`). Это разрешится само на
/// следующей попытке; объявить его фатальным значило бы уронить процесс на
/// гонке, которую создаёт наш собственный реконнект.
///
/// Эта гонка различается по коду состояния (`extract_sqlstate`), а не по
/// переводимой подстроке текста: код состояния не переводится никогда,
/// текст сообщения — переводится, когда у сервера локализован
/// `lc_messages`, и тогда подстрочная проверка молча перестала бы находить
/// гонку, превращая каждый её случай в фатальный выход. Подстрока
/// `"is active for PID"` остаётся запасным условием только на случай, если
/// строка ошибки почему-то не несёт SQLSTATE вовсе (например, будущая
/// версия крейта поменяет форматирование) — не потому, что она равноценна
/// коду; расчёт на неё как на основную проверку и есть то, что было
/// исправлено этим раундом.
///
/// Ограничение, которое эта функция в одиночку закрыть НЕ может: слот,
/// занятый ЧУЖИМ (не нашим) потребителем НАВСЕГДА, отвечает буквально тем же
/// `SQLSTATE 55006` — по одному коду состояния «наша прошлая сессия ещё не
/// отсоединилась» и «кто-то другой держит слот вечно» неотличимы, и
/// исключение выше классифицирует оба как восстановимые. Различитель этих
/// двух случаев физический, а не в коде состояния: наша прошлая сессия
/// отпускает слот за десятки миллисекунд (измерено, см.
/// `SlotBusyPatience`), чужой потребитель — нет. Именно поэтому эта функция
/// остаётся ЧИСТОЙ (без состояния, без времени) и не решает вопрос сама:
/// вызывающий (`classify_start_outcome`) оборачивает её решение бюджетом
/// терпения, накопленным по длительности, и эскалирует в
/// `PgcdcError::SlotBusyTimedOut`, когда терпение исчерпано.
fn classify_start_error(slot: &str, e: ReplicationError) -> PgcdcError {
    let reason = e.to_string();
    if e.is_transient() || is_busy_race_reason(&reason) {
        PgcdcError::Connection(format!("start replication: {e}"))
    } else {
        PgcdcError::SlotUnusable {
            slot: slot.to_owned(),
            reason,
        }
    }
}

/// Общий признак гонки "занят" (`SQLSTATE 55006`), вынесенный из
/// `classify_start_error` отдельно, чтобы `classify_start_outcome` мог
/// проверить его ДО того, как ошибка будет классифицирована и текст
/// сообщения станет недоступен без повторного `to_string()`.
fn is_busy_race_reason(reason: &str) -> bool {
    match extract_sqlstate(reason) {
        Some(code) => code == SLOT_BUSY_SQLSTATE,
        None => reason.contains("is active for PID"),
    }
}

/// Отслеживает, сколько времени ПОДРЯД слот отвечает гонкой "занят"
/// (`SQLSTATE 55006`). Код состояния не различает «наша прошлая сессия ещё
/// не отсоединилась» (разрешается за десятки миллисекунд) от «кто-то другой
/// держит слот навсегда» (не разрешается никогда) — единственный физический
/// различитель это ДЛИТЕЛЬНОСТЬ, поэтому бюджет терпения задан временем, а
/// не числом попыток: число попыток зависит от длины паузы бэкоффа, а не от
/// природы отказа.
///
/// Измерено (30 циклов «walsender держит слот → drop → тайминг до
/// следующего успешного `START_REPLICATION` с нуля, включая установление
/// нового соединения» — та же операция, что выполняет `stream_once` на
/// каждом реконнекте): 45–124мс, медиана ~76мс. Отдельно измерено сырое
/// время до сброса флага `pg_replication_slots.active`, без накладных
/// расходов нового соединения: 1.1–3.5мс, медиана ~1.8мс — то есть почти
/// весь след из первого замера это не задержка освобождения слота, а
/// накладные расходы TCP + аутентификации + `START_REPLICATION` самого
/// пробного соединения. Умолчание бюджета (`--slot-busy-budget-ms`,
/// 30000мс, `Config`) взято с запасом ~240× над худшим наблюдением полного
/// цикла реконнекта и ~8500× над сырым временем освобождения слота.
///
/// Счётчик ОБЯЗАН сбрасываться на ЛЮБОМ наблюдении, которое не является
/// гонкой "занят" — не только на успешном старте сессии
/// (`classify_start_outcome`, ветка `Ok`), но и на отказе другой природы
/// внутри той же функции, и на любом отказе `stream_once`, случившемся ДО
/// того, как классификация вообще состоялась: preflight слота, сверка
/// реконнекта, открытие соединения (`reset_patience_on_early_failure`).
/// Иначе долгоживущий процесс однажды упадёт из-за СУММЫ несвязанных между
/// собой эпизодов — например, гонки в момент ноль и другой гонки много позже,
/// разделённых часами недоступности сервера по совсем другой причине, — а не
/// из-за одного затянувшегося эпизода (I1, review round after task 4 finale:
/// раньше сбрасывалось ТОЛЬКО в Ok-ветке, и любой отказ, не дошедший до
/// классификации старта, часов не трогал).
struct SlotBusyPatience {
    first_seen: Option<Instant>,
}

impl SlotBusyPatience {
    fn new() -> Self {
        Self { first_seen: None }
    }

    /// Отмечает очередное наблюдение гонки "занят" в момент `now`. Возвращает
    /// `Some(waited)`, когда суммарная длительность с первого наблюдения
    /// достигла или превысила `budget` — вызывающий обязан считать это
    /// исчерпанием терпения и фатальной ошибкой.
    fn observe_busy(&mut self, now: Instant, budget: Duration) -> Option<Duration> {
        let first = *self.first_seen.get_or_insert(now);
        let waited = now.saturating_duration_since(first);
        (waited >= budget).then_some(waited)
    }

    /// Успешный старт сессии закрывает эпизод: несвязанные во времени гонки
    /// над месяцами работы долгоживущего процесса не должны суммироваться в
    /// один фатальный выход.
    fn reset(&mut self) {
        self.first_seen = None;
    }
}

/// Классифицирует исход `stream.start()` вместе с решением о терпении к
/// занятому слоту — хвост, общий для обоих полей задачи: `classify_start_error`
/// решает recoverable/fatal по ОДНОЙ попытке, эта функция добавляет
/// накопленное по ВРЕМЕНИ решение поверх него. Вынесена отдельно от
/// `stream_once`, чтобы мутация "убрать сброс терпения на успешном старте"
/// ловилась юнит-тестом уровня значений, а не только интеграционным
/// сценарием на реальном Postgres (тот же приём, что и у
/// `session_was_productive`/`ReconnectBackoff` выше).
fn classify_start_outcome(
    slot: &str,
    result: Result<(), ReplicationError>,
    patience: &mut SlotBusyPatience,
    budget: Duration,
    now: Instant,
) -> Result<(), PgcdcError> {
    let e = match result {
        Ok(()) => {
            patience.reset();
            return Ok(());
        }
        Err(e) => e,
    };
    if is_busy_race_reason(&e.to_string()) {
        if let Some(waited) = patience.observe_busy(now, budget) {
            return Err(PgcdcError::SlotBusyTimedOut {
                slot: slot.to_owned(),
                waited_ms: waited.as_millis() as u64,
                budget_ms: budget.as_millis() as u64,
            });
        }
        // Гонка ещё в бюджете: единственная ветка, которая обязана НЕ
        // трогать патиенс — иначе эпизод никогда бы не накопил достаточно
        // времени, чтобы вообще сработать.
        return Err(classify_start_error(slot, e));
    }
    // I1: отказ другой природы (не гонка "занят") закрывает открытый эпизод —
    // иначе он продолжил бы копиться во время отказа, никак с гонкой не
    // связанного, и сложился бы с последующим, тоже не связанным эпизодом
    // гонки в один фатальный выход.
    patience.reset();
    Err(classify_start_error(slot, e))
}

/// Общий хвост для ЛЮБОГО отказа `stream_once`, случившегося ДО того, как
/// `classify_start_outcome` успела классифицировать `stream.start()`:
/// preflight-проверка слота, сверка реконнекта, открытие самого соединения.
/// Ни один из этих отказов не может быть гонкой "занят" — SQLSTATE 55006
/// возвращает только ответ сервера на сам `START_REPLICATION`, — поэтому
/// такой отказ безусловно закрывает открытый эпизод терпения (I1, review
/// round after task 4 finale): без этого часы, накопленные прошлой гонкой,
/// продолжали бы идти всё то время, пока сервер недоступен по совсем другой
/// причине, и сложились бы с никак не связанным следующим эпизодом гонки в
/// один фатальный выход, которого по отдельности ни один из эпизодов не
/// заслужил. `Ok` здесь нарочно не трогает патиенс: успех preflight/сверки/
/// открытия соединения ещё не значит, что сессия стартовала — это решает
/// только `classify_start_outcome` дальше по `stream_once`.
fn reset_patience_on_early_failure<T>(
    result: Result<T, PgcdcError>,
    patience: &mut SlotBusyPatience,
) -> Result<T, PgcdcError> {
    if result.is_err() {
        patience.reset();
    }
    result
}

/// Была ли только что закончившаяся сессия продуктивной для целей сброса
/// бэкоффа реконнекта. Читает ПОДТВЕРЖДЁННУЮ трекером позицию (`acked`), а
/// не принятую (`received`), намеренно: и групповой барьер, и keepalive-
/// продвижение на простаивающей публикации двигают `acked`, а `received`
/// реагирует только на приход кадра данных. Вынесена в отдельную функцию,
/// принимающую сам трекер, а не голые `Lsn`, ровно затем, чтобы это чтение
/// можно было закрепить юнит-тестом на этом уровне: живое доказательство,
/// что расхождение реально, — спокойный прогон, где сводка показала `acked`
/// продвинутым при `received` на нуле, keepalive подтвердил WAL, не приняв
/// ни одного кадра (review Task 2, round 1, F1; review Task 3, round 1,
/// F3).
fn session_was_productive(tracker: &LsnTracker, acked_before: Lsn) -> bool {
    tracker.acked() > acked_before
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

/// Хвост, общий для обоих способов доказать durable-позицию: барьер
/// (`Sink::flush`) и keepalive-продвижение доказывают её по-разному, но раз
/// позиция решена, дальше оба места отмечают её, подтверждают трекером и
/// отправляют feedback — дословно одинаковыми четырьмя шагами. Барьер
/// НЕ входит сюда и не может: только вызывающий решает, что считать
/// durable, эта функция лишь записывает решение (review Task 3, round 1,
/// F1 — иначе keepalive-путь мог бы незаметно приобрести барьер).
///
/// Возвращает ПОДТВЕРЖДЁННУЮ трекером позицию (acked), а не переданный
/// `durable`: сегодня они совпадают, но с реконнектом внутри процесса
/// (следующий этап) replay уже подтверждённой транзакции может отличаться —
/// отправить в feedback не то, что подтвердил трекер, значило бы откатить
/// сервер назад. Подтверждаем acked, НЕ commit_lsn: commit_lsn указывает на
/// начало записи коммита, и рестарт перечитал бы ту же транзакцию.
async fn acknowledge_durable(
    state: &mut SessionState,
    stream: &mut LogicalReplicationStream,
    durable: Lsn,
    metrics: &Arc<Metrics>,
) -> Result<Lsn, PgcdcError> {
    // Порядок держит инвариант 1 в этой точке вызова по построению:
    // `note_durable` только что подняла durable как минимум до `durable`,
    // так что `try_ack(durable)` ниже отказать здесь не может — guard
    // внутри `try_ack` не выполняет живую работу на ЭТОМ пути. Он остаётся
    // защитой не для этого места, а для будущего вызывающего, который
    // позовёт `try_ack`, пропустив отметку durable.
    state.tracker.note_durable(durable);
    state.tracker.try_ack(durable)?;
    let acked = state.tracker.acked();

    // Единственное место записи этой позиции (см. бриф задачи 2): и
    // групповой барьер, и keepalive-продвижение проходят через этот общий
    // хвост, так что второго сайта записи не появится ни у одного из них.
    metrics.set_last_acknowledged_lsn(acked.0);

    stream.shared_lsn_feedback.update_flushed_lsn(acked.0);
    stream.shared_lsn_feedback.update_applied_lsn(acked.0);

    // Обязательство Q25(в): без явного вызова подтверждение уходит
    // с задержкой 18–22 с по внутреннему расписанию крейта.
    stream
        .send_feedback()
        .await
        .map_err(|e| PgcdcError::Connection(format!("send_feedback: {e}")))?;

    Ok(acked)
}

/// Доводит принятое sink'ом до барьера и, если было что подтверждать,
/// прогоняет результат через общий хвост `acknowledge_durable`. Общий код
/// для группового таймера и для завершения по сигналу: без извлечения в
/// отдельную функцию эти два места разошлись бы, а мутационное покрытие,
/// снятое против таймерной ветки, не защищало бы вторую копию (см. бриф
/// задачи 3).
async fn flush_and_acknowledge(
    sink: &mut Box<dyn Sink>,
    state: &mut SessionState,
    stream: &mut LogicalReplicationStream,
    metrics: &Arc<Metrics>,
) -> Result<(), PgcdcError> {
    // Отметить durable имеет право только успешный барьер, а не приём записи.
    if let Some(durable) = sink.flush().await? {
        let acked = acknowledge_durable(state, stream, durable, metrics).await?;
        debug!(lsn = %acked, "group_acknowledged");
    }
    Ok(())
}

pub async fn run(
    config: Config,
    mut sink: Box<dyn Sink>,
    metrics: Arc<Metrics>,
) -> Result<(), PgcdcError> {
    // Первым делом — до любого подключения и любого лога, где могла бы всплыть строка.
    config.database_url.validate()?;
    config.validate_reconnect_bounds()?;

    let mut state = SessionState::new(config.max_transaction_events);
    let mut backoff = ReconnectBackoff::new(
        Duration::from_millis(config.reconnect_initial_ms),
        Duration::from_millis(config.reconnect_max_ms),
    );
    let mut attempt: u32 = 0;

    // Живёт РЯДОМ с `backoff`, а не внутри `SessionState`: `reset_for_reconnect`
    // зовётся на КАЖДОМ обрыве соединения независимо от того, был ли этот
    // обрыв гонкой "занят" — если бы терпение жило там, оно обнулялось бы на
    // каждой попытке и никогда не смогло бы накопить достаточно времени,
    // чтобы вообще сработать (SlotBusyPatience).
    let mut slot_busy_patience = SlotBusyPatience::new();

    // Флаг создаётся один раз ДО внешнего цикла и передаётся одной и той же
    // ссылкой в каждую сессию: если создавать его заново на каждом реконнекте,
    // после первого обрыва процесс перестал бы реагировать на сигнал.
    let shutdown = spawn_shutdown_listener();

    // Тем же приёмом, что и `shutdown`: отсчёт до сводки создаётся один раз
    // ДО внешнего цикла и передаётся одной и той же ссылкой в каждую сессию.
    // Счётчики, которые сводка печатает, процессные и переживают реконнект;
    // если заводить отсчёт заново внутри `stream_once` на каждой сессии,
    // процесс, переподключающийся чаще `METRICS_REPORT_INTERVAL`, никогда не
    // проживёт достаточно долго внутри одной сессии, чтобы сводка вообще
    // вышла (review Task 3, round 1, F1) — именно в этой ситуации она нужнее
    // всего, потому что `reconnects`/`errors` в строке существуют ради неё.
    let mut last_report = tokio::time::Instant::now();

    loop {
        // I1: первое из двух мест, где внешний цикл реконнекта смотрит на
        // флаг завершения (второе — нарезанная пауза бэкоффа чуть ниже).
        // Нарезка проверяет флаг ПЕРЕД каждым куском и ни разу ПОСЛЕ
        // последнего — сигнал, попавший именно в последний кусок паузы, до
        // неё не доходит. Эта проверка и есть то место, которое его ловит:
        // без неё такой сигнал стоит одной лишней, не ограниченной по
        // времени попытки подключения (`stream_once` заново пройдёт
        // preflight) — против отказанного порта мгновенно, а против
        // адреса, который не отвечает вовсе, растягивается на длительность
        // таймаута соединения (см. `spawn_shutdown_listener`). Та же
        // проверка ловит и сигнал, пришедший до самой первой попытки —
        // раньше, чем цикл вообще побывал внутри `stream_once`.
        //
        // Сигнал во внешнем цикле. Выходим с нулём, но НЕ потому, что доводить
        // нечего — после обрыва посреди окна подтверждения в буфере писателя
        // вполне может лежать принятая, но не слитая транзакция, и этот путь
        // пропускает слив, который делает ветка внутри сессии. Ноль корректен
        // по другой причине: непроведённое через барьер не было и подтверждено,
        // поэтому слот отдаст его заново, а дубликаты разрешает инвариант 2.
        // Терять здесь нечего, и это не то же самое, что «нечего доводить».
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        let acked_before = state.tracker.acked();

        match stream_once(
            &config,
            &mut sink,
            &mut state,
            &shutdown,
            &metrics,
            &mut last_report,
            &mut slot_busy_patience,
        )
        .await
        {
            Ok(SessionOutcome::ShutdownRequested) => return Ok(()),
            Ok(SessionOutcome::Disconnected) => {}
            // Восстановимые ошибки ведут в реконнект, фатальные — наружу.
            // Классификация живёт в типе (`is_fatal`), а не в разборе текста.
            Err(e) if !e.is_fatal() => {
                warn!(error = %e, error_kind = e.kind(), "postgres_connection_lost");
                metrics.add_error();
            }
            Err(e) => return Err(e),
        }

        // Признак продуктивности вынесен в `session_was_productive` (review
        // Task 3, round 1, F3): решение о том, что считать продуктивностью,
        // читает acked, а не received (review Task 2, round 1, F1), и это
        // чтение закреплено юнит-тестом на уровне самой функции, а не только
        // косвенно через интеграционный сценарий.
        let productive = session_was_productive(&state.tracker, acked_before);
        if productive {
            attempt = 0;
        }
        attempt += 1;
        let delay = backoff.next_delay(productive);
        metrics.add_reconnect();
        warn!(
            retry = attempt,
            backoff_ms = delay.as_millis() as u64,
            "reconnecting"
        );

        // I1: второе из двух мест (первое — проверка в начале прохода
        // выше). Пауза нарезана на куски по SHUTDOWN_POLL_INTERVAL вместо
        // одного sleep(delay) — иначе сигнал, пришедший посреди паузы
        // длиной вплоть до reconnect_max_ms (по умолчанию 30с), был бы
        // замечен только по её истечении. Как и в проверке выше, ноль
        // здесь корректен не потому, что нечего доводить, а потому, что
        // недоведённое до барьера не было подтверждено — слот отдаст его
        // заново, а дубликаты разрешает инвариант 2.
        let mut remaining = delay;
        while remaining > Duration::ZERO {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(());
            }
            let chunk = remaining.min(SHUTDOWN_POLL_INTERVAL);
            tokio::time::sleep(chunk).await;
            remaining = remaining.saturating_sub(chunk);
        }

        // Кэш и сборщик сбрасываются, позиции переносятся, датчик буфера
        // обнуляется вместе с ними (F1, review Task 2, round 1).
        state.reset_for_reconnect(&metrics);
    }
}

/// Верхняя граница ожидания в чтении — и тем самым верхняя граница задержки
/// реакции на флаг завершения. НЕ связывать с `ack_interval_ms`: тот задаёт
/// расписание барьера и не должен диктовать, как быстро процесс замечает
/// сигнал. Раньше чтение было ограничено самим `ack_interval`, поэтому флаг
/// проверялся не чаще периодического барьера — при проде на несколько
/// секунд это делало задержку штатной остановки равной длине интервала
/// подтверждения, и супервизор с коротким grace period убивал процесс
/// раньше, чем тот вообще замечал сигнал (review Task 3, round 2, F2). При
/// значении по умолчанию (`ack_interval_ms = 200`) цикл и так просыпался с
/// этой частотой — константа ничего не удорожает, только отвязывает частоту
/// пробуждения от периода барьера.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Как часто выходит сводная строка. Не конфигурируется: это не поведение, а
/// громкость, и десять секунд — компромисс между «видно, что процесс жив» и
/// «лог не забивается» (DECISIONS Q23).
const METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Одна сессия репликации: preflight, подключение, цикл. Возвращается при
/// обрыве соединения или при штатном завершении.
async fn stream_once(
    config: &Config,
    sink: &mut Box<dyn Sink>,
    state: &mut SessionState,
    shutdown: &Arc<AtomicBool>,
    metrics: &Arc<Metrics>,
    last_report: &mut tokio::time::Instant,
    slot_busy_patience: &mut SlotBusyPatience,
) -> Result<SessionOutcome, PgcdcError> {
    // Захватываем ДО preflight, а не проверяем `state.durable()` заново
    // позже: решение "это реконнект" принимается на входе в функцию и не
    // должно незаметно подстроиться под то, что случится дальше внутри неё
    // (review Task 2, round 1, F7).
    let reconnecting = is_reconnect(state.durable());

    // Обязательство Q25(а): guard ДО start(), потому что start() безусловно
    // зовёт ensure_replication_slot() и при отсутствующем слоте молча создаст
    // новый на текущей позиции WAL, потеряв всё закоммиченное раньше.
    // I1: обёрнуто в reset_patience_on_early_failure, а не голый `?` — отказ
    // здесь физически не может быть гонкой "занят" (тот код приходит только
    // в ответ на START_REPLICATION дальше), поэтому обязан безусловно
    // закрывать любой открытый эпизод терпения, а не оставлять его часы
    // тикать, пока сервер недоступен по совсем другой причине.
    let info_slot = reset_patience_on_early_failure(
        preflight_slot(config.database_url.expose(), &config.slot).await,
        slot_busy_patience,
    )?;
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
    // продолжаем, промежуток перечитается дубликатами — это разрешает
    // инвариант 2 (DECISIONS §1) вместе со строкой транспортных
    // обязательств спайка (DECISIONS Q25).
    if reconnecting {
        // I1: тем же приёмом, что и preflight выше — сверка реконнекта не
        // может вернуть гонку "занят", только SlotAhead или ничего.
        if let Some(warning) = reset_patience_on_early_failure(
            check_reconnect(&config.slot, &info_slot, state.durable()),
            slot_busy_patience,
        )? {
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
    // I1: тем же приёмом — открытие TCP-соединения тоже не может вернуть
    // гонку "занят", она приходит только в ответ на сам START_REPLICATION.
    let mut stream = reset_patience_on_early_failure(
        LogicalReplicationStream::new(&url, stream_config)
            .await
            .map_err(|e| PgcdcError::Connection(format!("open replication stream: {e}"))),
        slot_busy_patience,
    )?;

    // start_lsn = None означает 0/0: сервер возьмёт confirmed_flush_lsn слота.
    // Слот — единственный источник истины (DECISIONS Q4, Q19).
    //
    // classify_start_outcome оборачивает classify_start_error бюджетом
    // терпения к занятому слоту (SlotBusyPatience): гонка "занят" сама по
    // себе остаётся восстановимой, но если она тянется дольше
    // `slot_busy_budget_ms` суммарно, это эскалируется в фатальный
    // SlotBusyTimedOut — единственный сигнал, отличающий вечно занятый
    // чужим потребителем слот от мгновенно разрешающейся гонки с нашей же
    // прошлой сессией, это ДЛИТЕЛЬНОСТЬ, а не код ошибки.
    classify_start_outcome(
        &config.slot,
        stream.start(None).await,
        slot_busy_patience,
        Duration::from_millis(config.slot_busy_budget_ms),
        Instant::now(),
    )?;
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
            flush_and_acknowledge(sink, state, &mut stream, metrics).await?;
            info!("shutdown_requested");
            return Ok(SessionOutcome::ShutdownRequested);
        }

        // Сводка на INFO раз в METRICS_REPORT_INTERVAL — не на каждое событие
        // (то на DEBUG, ниже). Стоит на входе в оборот цикла, вне порядка
        // запись→processed→(таймер)барьер→durable→ack→feedback, потому что
        // только читает снимок и ни на что не влияет (§16, DECISIONS Q23).
        if last_report.elapsed() >= METRICS_REPORT_INTERVAL {
            *last_report = tokio::time::Instant::now();
            let s = metrics.snapshot();
            info!(
                events = s.events_total,
                transactions = s.transactions_total,
                bytes = s.bytes_received_total,
                reconnects = s.reconnects_total,
                errors = s.errors_total,
                last_received_lsn = %Lsn(s.last_received_lsn),
                last_acknowledged_lsn = %Lsn(s.last_acknowledged_lsn),
                buffer = s.transaction_buffer_size,
                "metrics_report"
            );
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
        //
        // Таймаут — SHUTDOWN_POLL_INTERVAL, а не ack_interval: барьер копит
        // события по своему расписанию (elapsed-проверка ниже), а это
        // ограничение существует только для того, чтобы не проспать флаг
        // завершения дольше положенного.
        let read =
            tokio::time::timeout(SHUTDOWN_POLL_INTERVAL, stream.next_raw_event(&cancel)).await;

        match read {
            Ok(Ok(raw)) => {
                state.tracker.note_received(Lsn(raw.wal_end.0));
                metrics.add_bytes(raw.data.len() as u64);
                metrics.set_last_received_lsn(raw.wal_end.0);

                let msg = decode(&raw.data)?;
                // F4 (review Task 2, round 1): захватываем длину буфера ДО
                // `?`, а не только независимо от Some/None результата — иначе
                // ошибка внутри `handle` пропускает обновление датчика вовсе,
                // и он остаётся при последнем значении из прошлого кадра.
                let handled = state
                    .assembler
                    .handle(msg, Lsn(raw.wal_start.0), &mut state.cache);
                metrics.set_transaction_buffer_size(state.assembler.len() as u64);
                if let Some(tx) = handled? {
                    let changes = tx.changes.len();
                    let end_lsn = tx.end_lsn;

                    // Порядок нерушим: сначала sink, потом барьер, потом durable, только потом ack.
                    sink.write_transaction(&tx).await?;
                    state.tracker.note_processed(end_lsn);
                    metrics.add_transaction();
                    metrics.add_events(changes as u64);
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
            flush_and_acknowledge(sink, state, &mut stream, metrics).await?;
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
            let acked = acknowledge_durable(state, &mut stream, server_lsn, metrics).await?;
            debug!(lsn = %acked, "advanced_from_keepalive");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_replication_rejected_by_the_server_is_fatal_wrong_plugin() {
        // Дешёвая ветка C1: слот несёт чужой output-плагин, сервер отвечает
        // "option \"proto_version\" = \"1\" is unknown" (SQLSTATE 22023), а
        // pg_walstream заворачивает это в Protocol (не транзиентный вариант).
        // Строка сконструирована как настоящая: "(SQLSTATE 22023)" в хвосте —
        // ровно то, что кладёт туда PgErrorFields::Display крейта транспорта
        // (connection/native/error.rs), а не синтетическое упрощение.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  option \"proto_version\" = \"1\" is unknown (SQLSTATE 22023)",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::SlotUnusable { .. }), "{err:?}");
        assert!(err.is_fatal());
    }

    #[test]
    fn start_replication_rejected_by_the_server_is_fatal_invalidated_slot() {
        // Дорогая ветка C1: слот инвалидирован превышением
        // max_slot_wal_keep_size, сервер отвечает SQLSTATE 55000 (дословно
        // воспроизведено живым прогоном в task-4-report.md). Тот же
        // Protocol-конверт, тот же вердикт.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  can no longer get changes from replication slot \"pgcdc_slot\" (SQLSTATE 55000)",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::SlotUnusable { .. }), "{err:?}");
        assert!(err.is_fatal());
    }

    #[test]
    fn start_replication_transport_drop_stays_recoverable() {
        // Обрыв связи (сокет, а не ответ сервера) обязан остаться
        // восстановимым — иначе тесты на реконнект покраснели бы (см. отчёт
        // по C1: мутация "сделать транспортный обрыв фатальным").
        let e = ReplicationError::transient_connection("connection reset by peer");
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn start_replication_slot_still_held_by_our_own_prior_session_stays_recoverable() {
        // Сервер тоже ОТВЕТИЛ, но это не про непригодность слота — предыдущий
        // walsender ещё не отпустил его. Разрешится само на следующей
        // попытке. SQLSTATE 55006 в хвосте строки — настоящий код гонки
        // (ERRCODE_OBJECT_IN_USE), различение обязано опираться на него, а
        // не на подстроку "is active for PID" (P1, re-review round after
        // task 4): без него в строке этот тест ловил бы только запасной путь.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  replication slot \"pgcdc_slot\" is active for PID 4242 (SQLSTATE 55006)",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn start_replication_slot_busy_race_is_recognized_without_sqlstate_via_fallback() {
        // Запасной путь: строка не несёт SQLSTATE вовсе (гипотетическая
        // будущая версия крейта поменяла форматирование, или ошибка пришла
        // не через PgErrorFields). Подстрока остаётся резервным условием —
        // именно оно и обязано сработать здесь.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  replication slot \"pgcdc_slot\" is active for PID 4242",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn start_replication_wrong_sqlstate_with_the_race_substring_is_still_fatal() {
        // Когда SQLSTATE присутствует, но не совпадает с гонкой (55006), он
        // обязан решать — даже если по случайности в тексте тоже нашлась бы
        // подстрока "is active for PID" где-то дальше по сообщению (DETAIL,
        // например). Проверяем, что primary-путь не даёт запасному пути
        // перекрыть себя.
        let e = ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  can no longer get changes from replication slot \"pgcdc_slot\" (SQLSTATE 55000)\nDETAIL: another slot is active for PID 4242 elsewhere",
        );
        let err = classify_start_error("pgcdc_slot", e);
        assert!(matches!(err, PgcdcError::SlotUnusable { .. }), "{err:?}");
        assert!(err.is_fatal());
    }

    /// Строит ту же самую ошибку гонки "занят", что и живой прогон против
    /// реального Postgres (см. `start_replication_slot_still_held_by_our_own_prior_session_stays_recoverable`
    /// выше и `task-4-report.md`).
    fn busy_race_error() -> ReplicationError {
        ReplicationError::protocol(
            "START_REPLICATION did not enter COPY mode: ERROR:  replication slot \"pgcdc_slot\" is active for PID 4242 (SQLSTATE 55006)",
        )
    }

    #[test]
    fn slot_busy_patience_does_not_trigger_before_the_budget_elapses() {
        let mut p = SlotBusyPatience::new();
        let t0 = Instant::now();
        let budget = Duration::from_millis(1000);
        assert!(p.observe_busy(t0, budget).is_none());
        assert!(p
            .observe_busy(t0 + Duration::from_millis(999), budget)
            .is_none());
    }

    #[test]
    fn slot_busy_patience_triggers_once_the_budget_elapses() {
        let mut p = SlotBusyPatience::new();
        let t0 = Instant::now();
        let budget = Duration::from_millis(1000);
        assert!(p.observe_busy(t0, budget).is_none());
        let waited = p
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .expect("бюджет исчерпан");
        assert_eq!(waited, Duration::from_millis(1000));
    }

    #[test]
    fn slot_busy_patience_reset_forgets_the_accumulated_duration() {
        let mut p = SlotBusyPatience::new();
        let t0 = Instant::now();
        let budget = Duration::from_millis(1000);
        assert!(p.observe_busy(t0, budget).is_none());
        p.reset();
        // Без сброса это наблюдение (ровно на границе бюджета от t0) сработало бы.
        assert!(p
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .is_none());
    }

    #[test]
    fn classify_start_outcome_stays_recoverable_while_the_busy_race_is_within_budget() {
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(matches!(err, PgcdcError::Connection(_)), "{err:?}");
        assert!(!err.is_fatal());
    }

    #[test]
    fn classify_start_outcome_escalates_to_fatal_once_the_busy_race_outlives_the_budget() {
        // Ровно то, что видел живой прогон "что осталось открытым" в
        // task-4-report.md: 34 цикла подряд SQLSTATE 55006 без единого
        // ненулевого кода выхода. Здесь тот же наблюдаемый ряд ошибок, но
        // растянутый по времени дольше бюджета — эскалация обязана
        // сработать.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(
            !err.is_fatal(),
            "первое наблюдение не должно быть фатальным"
        );

        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(1500),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::SlotBusyTimedOut { .. }),
            "{err:?}"
        );
        assert!(err.is_fatal());
    }

    #[test]
    fn a_successful_start_resets_the_slot_busy_patience_so_unrelated_episodes_dont_sum() {
        // Отдельный тест на требование "счётчик терпения обязан сбрасываться
        // на успешном старте сессии": без сброса в Ok-ветке
        // `classify_start_outcome` эпизод из первого наблюдения продолжил бы
        // копиться и после успешной сессии, и второй, никак не связанный с
        // первым эпизод сложился бы с ним в один фатальный выход.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        // Эпизод 1: одно наблюдение гонки, бюджет ещё не исчерпан.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal());

        // Сессия успешно стартует 900мс спустя — обязана закрыть эпизод 1.
        classify_start_outcome(
            "pgcdc_slot",
            Ok(()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(900),
        )
        .expect("успешный старт не может быть ошибкой");

        // Эпизод 2 начинается 1800мс после t0 — то есть 900мс после сброса.
        // Без сброса это наблюдение унаследовало бы first_seen = t0 и уже
        // превысило бы бюджет (1800мс >= 1000мс). Со сбросом это новый,
        // самостоятельный эпизод, ещё далёкий от бюджета.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(1800),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::Connection(_)),
            "сброс обязан был начать эпизод 2 заново: {err:?}"
        );
        assert!(!err.is_fatal());
    }

    #[test]
    fn a_non_busy_failure_inside_classify_start_outcome_also_resets_the_patience() {
        // I1: раньше сбрасывалось ТОЛЬКО в Ok-ветке. Отказ другой природы
        // (например, оборвавшаяся связь во время самого START_REPLICATION)
        // тоже обязан закрыть открытый эпизод — иначе он продолжил бы
        // копиться и сложился бы с никак не связанным следующим эпизодом
        // гонки в один фатальный выход.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        // Эпизод 1: одно наблюдение гонки, бюджет ещё далёк.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal());

        // Отказ другой природы 900мс спустя — НЕ гонка "занят". Обязан
        // закрыть эпизод 1, а не пройти мимо патиенса.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(ReplicationError::transient_connection(
                "connection reset by peer",
            )),
            &mut patience,
            budget,
            t0 + Duration::from_millis(900),
        )
        .unwrap_err();
        assert!(!err.is_fatal(), "отказ другой природы сам не фатален");

        // Эпизод 2 начинается 1800мс после t0 — без сброса унаследовал бы
        // first_seen = t0 и уже превысил бы бюджет (1800мс >= 1000мс).
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(1800),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::Connection(_)),
            "отказ другой природы обязан был начать эпизод 2 заново: {err:?}"
        );
        assert!(!err.is_fatal());
    }

    #[test]
    fn reset_patience_on_early_failure_closes_an_open_episode_on_err() {
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        assert!(patience.observe_busy(t0, budget).is_none(), "эпизод открыт");

        let unreachable: Result<(), PgcdcError> =
            Err(PgcdcError::Connection("preflight connect: refused".into()));
        reset_patience_on_early_failure(unreachable, &mut patience).unwrap_err();

        // Без сброса это наблюдение (ровно на границе бюджета от t0)
        // сработало бы — тем же приёмом, что и `SlotBusyPatience::reset`.
        assert!(patience
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .is_none());
    }

    #[test]
    fn reset_patience_on_early_failure_leaves_an_open_episode_alone_on_ok() {
        // Успех preflight/сверки/открытия соединения ещё не значит, что
        // сессия стартовала — закрывать эпизод здесь на Ok было бы неверно:
        // решение "старт успешен" принимает только classify_start_outcome.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();
        assert!(patience.observe_busy(t0, budget).is_none());

        reset_patience_on_early_failure(Ok(()), &mut patience).unwrap();

        let waited = patience
            .observe_busy(t0 + Duration::from_millis(1000), budget)
            .expect("эпизод обязан был остаться открытым");
        assert_eq!(waited, Duration::from_millis(1000));
    }

    #[test]
    fn a_busy_episode_does_not_survive_an_unrelated_pre_start_failure_in_between() {
        // I1 reproduction (дословно из ревью): гонка "занят" в момент ноль;
        // затем сервер недоступен — каждая попытка падает РАНЬШЕ, чем
        // доходит до classify_start_outcome (preflight слота или открытие
        // соединения, а не ответ на START_REPLICATION); затем сервер
        // вернулся, и наш же прежний walsender снова держит слот 76мс
        // (измеренная медиана, см. SlotBusyPatience). Второй эпизод не
        // должен унаследовать часы первого — суммирования быть не должно.
        let mut patience = SlotBusyPatience::new();
        let budget = Duration::from_millis(1000);
        let t0 = Instant::now();

        // Эпизод 1: гонка "занят" в момент ноль, бюджет ещё далёк.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0,
        )
        .unwrap_err();
        assert!(!err.is_fatal());

        // Сервер недоступен: отказ до классификации старта (preflight/
        // открытие соединения), 5 секунд спустя — то же самое, что стоит
        // на пути stream_once ДО classify_start_outcome.
        let unreachable: Result<(), PgcdcError> =
            Err(PgcdcError::Connection("preflight connect: refused".into()));
        reset_patience_on_early_failure(unreachable, &mut patience).unwrap_err();

        // Эпизод 2: сервер снова отвечает гонкой "занят" ещё 76мс спустя.
        // Суммарно от t0 прошло бы 5076мс — далеко за бюджетом, и БЕЗ
        // сброса это наблюдение эскалировало бы в SlotBusyTimedOut ошибочно:
        // процесс, который восстановился бы со следующей попытки, умер бы.
        let err = classify_start_outcome(
            "pgcdc_slot",
            Err(busy_race_error()),
            &mut patience,
            budget,
            t0 + Duration::from_millis(5000) + Duration::from_millis(76),
        )
        .unwrap_err();
        assert!(
            matches!(err, PgcdcError::Connection(_)),
            "второй эпизод не должен унаследовать часы первого: {err:?}"
        );
        assert!(
            !err.is_fatal(),
            "несвязанные эпизоды не должны суммироваться в фатальный выход"
        );
    }

    #[test]
    fn extract_sqlstate_reads_the_code_from_pg_walstreams_error_formatting() {
        // Формат подтверждён чтением исходника крейта
        // (connection/native/error.rs::PgErrorFields::Display) и живым
        // прогоном на реальном Postgres (task-4-report.md).
        assert_eq!(
            extract_sqlstate(
                "ERROR:  can no longer get changes from replication slot \"s\" (SQLSTATE 55000)"
            ),
            Some("55000")
        );
    }

    #[test]
    fn extract_sqlstate_is_none_when_absent() {
        assert_eq!(extract_sqlstate("connection reset by peer"), None);
    }

    #[test]
    fn is_reconnect_is_false_on_a_cold_start() {
        assert!(!is_reconnect(Lsn(0)));
    }

    #[test]
    fn is_reconnect_is_true_once_something_is_durable() {
        assert!(is_reconnect(Lsn(0x1000)));
    }

    #[test]
    fn session_is_productive_when_acked_advances_via_keepalive_without_new_frames() {
        // Живое доказательство расхождения (review Task 3, round 1, F3): в
        // спокойном прогоне сводка показала подтверждённую позицию
        // продвинутой при принятой на нуле — keepalive подтвердил WAL, не
        // приняв ни одного кадра. Признак обязан считать эту сессию
        // продуктивной, а мутация, подменяющая acked на received внутри
        // `session_was_productive`, этот тест провалит.
        let mut t = LsnTracker::new();
        let acked_before = t.acked();
        t.note_durable(Lsn(0x1000));
        t.try_ack(Lsn(0x1000)).unwrap();
        assert_eq!(t.received(), Lsn(0), "ни один кадр не был принят");
        assert!(session_was_productive(&t, acked_before));
    }

    #[test]
    fn session_is_not_productive_when_only_received_moves() {
        // Обратная сторона того же расхождения: кадр пришёл (received ушёл
        // вперёд), но барьер его ещё не подтвердил — по acked сессия
        // непродуктивна, и признак обязан согласиться с acked, а не с
        // received.
        let mut t = LsnTracker::new();
        let acked_before = t.acked();
        t.note_received(Lsn(0x1000));
        assert!(!session_was_productive(&t, acked_before));
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
    fn backoff_keeps_growing_across_unproductive_attempts() {
        // Закрывает разрыв, который переживал прежний набор тестов (review
        // Task 3, round 1, F2): мутация «сделать сброс безусловным» —
        // `next_delay` всегда обнуляет `current` вне зависимости от
        // `productive` — оставляла зелёными оба существующих теста на
        // бэкофф, потому что ни один из них не смотрит на промежуточные
        // значения при `productive = false`. Под этой мутацией каждый вызов
        // с `productive = false` тоже возвращал бы начальную задержку
        // (100мс) навсегда — вечная долбёжка мёртвого сервера каждые сто
        // миллисекунд вместо экспоненты. Этот тест читает именно
        // промежуточные значения серии непродуктивных попыток на уровне
        // метода типа, а не свободной функции `next_backoff`.
        let mut b = ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1000));
        assert_eq!(b.next_delay(false), Duration::from_millis(100));
        assert_eq!(b.next_delay(false), Duration::from_millis(200));
        assert_eq!(b.next_delay(false), Duration::from_millis(400));
        assert_eq!(b.next_delay(false), Duration::from_millis(800));
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

        s.reset_for_reconnect(&Metrics::new());

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

        s.reset_for_reconnect(&Metrics::new());

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
        s.reset_for_reconnect(&Metrics::new());
        s.tracker.note_processed(Lsn(0x1000));
        assert_eq!(s.tracker.processed(), Lsn(0x2000));
    }

    #[test]
    fn reconnect_zeroes_the_buffer_gauge_even_with_an_open_transaction() {
        // F1 (review Task 2, round 1): сброс на реконнекте не проходит через
        // приёмную ветку stream_once, где обычно выставляется этот датчик, —
        // он обязан обнулить его сам. Без этого на простаивающей после
        // обрыва публикации датчик держал бы последнее ненулевое значение
        // бесконечно, вместо того чтобы честно показать пустой буфер новой
        // сессии.
        let mut s = SessionState::new(1000);
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
        let metrics = Metrics::new();
        metrics.set_transaction_buffer_size(5);

        s.reset_for_reconnect(&metrics);

        assert_eq!(
            metrics.snapshot().transaction_buffer_size,
            0,
            "датчик обязан упасть до нуля вместе со сбросом сборщика"
        );
    }
}
