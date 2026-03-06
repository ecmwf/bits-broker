use std::fmt;

use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

pub struct PrettyFormat;

impl<S, N> FormatEvent<S, N> for PrettyFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let level = *event.metadata().level();

        // HH:MM:SS dimmed
        let now = chrono::Local::now();
        write!(writer, "\x1b[2m{}\x1b[0m ", now.format("%H:%M:%S"))?;

        // Level badge — bright colours
        let (colour, label) = match level {
            Level::TRACE => ("\x1b[35m", "TRACE"),  // magenta
            Level::DEBUG => ("\x1b[34m", "DEBUG"),  // blue
            Level::INFO  => ("\x1b[92m", " INFO"),  // bright green
            Level::WARN  => ("\x1b[93m", " WARN"),  // bright yellow
            Level::ERROR => ("\x1b[91m", "ERROR"),  // bright red
        };
        write!(writer, "{colour}\x1b[1m{label}\x1b[0m  ")?;

        // Span fields (e.g. job.id=...) in bright cyan, from outermost to innermost
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let ext = span.extensions();
                if let Some(fields) = ext.get::<FormattedFields<N>>() {
                    if !fields.is_empty() {
                        write!(writer, "\x1b[96m[{}]\x1b[0m ", fields)?;
                    }
                }
            }
        }

        // Message and any extra fields on the event itself
        ctx.format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}
