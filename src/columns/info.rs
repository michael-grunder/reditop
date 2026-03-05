use crate::column::{
    Align, CellText, Column, FormatSpec, RenderCtx, SortCtx, SortKey, ValueType, WidthHint,
    format_bytes, format_millis, format_percent, parse_bool, parse_f64, parse_i64, parse_string,
    parse_u64,
};

pub struct RedisInfoFieldColumn {
    pub header: String,
    pub info_key: String,
    pub value_type: ValueType,
    pub format: FormatSpec,
    pub missing: String,
    pub align: Align,
    pub width_hint: WidthHint,
}

impl RedisInfoFieldColumn {
    fn format_value(&self, snap: &crate::model::InstanceState) -> Option<String> {
        match self.value_type {
            ValueType::String => {
                parse_string(snap, &self.info_key).map(|value| match self.format {
                    FormatSpec::Raw => value,
                    _ => value,
                })
            }
            ValueType::U64 => parse_u64(snap, &self.info_key).map(|value| self.apply_u64(value)),
            ValueType::I64 => parse_i64(snap, &self.info_key).map(|value| self.apply_i64(value)),
            ValueType::F64 | ValueType::Percent => {
                parse_f64(snap, &self.info_key).map(|value| self.apply_f64(value))
            }
            ValueType::Bytes => parse_u64(snap, &self.info_key).map(|value| self.apply_u64(value)),
            ValueType::Bool => parse_bool(snap, &self.info_key).map(|value| {
                if value {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }),
        }
    }

    fn apply_u64(&self, value: u64) -> String {
        match self.format {
            FormatSpec::Raw => value.to_string(),
            FormatSpec::BytesHuman => format_bytes(value),
            FormatSpec::Fixed(decimals) => format!("{:.*}", decimals as usize, value as f64),
            FormatSpec::Percent(decimals) => format_percent(value as f64, decimals),
            FormatSpec::Millis(decimals) => format_millis(value as f64, decimals),
        }
    }

    fn apply_i64(&self, value: i64) -> String {
        match self.format {
            FormatSpec::Raw => value.to_string(),
            FormatSpec::BytesHuman => format_bytes(value.max(0) as u64),
            FormatSpec::Fixed(decimals) => format!("{:.*}", decimals as usize, value as f64),
            FormatSpec::Percent(decimals) => format_percent(value as f64, decimals),
            FormatSpec::Millis(decimals) => format_millis(value as f64, decimals),
        }
    }

    fn apply_f64(&self, value: f64) -> String {
        match self.format {
            FormatSpec::Raw => value.to_string(),
            FormatSpec::BytesHuman => format_bytes(value.max(0.0) as u64),
            FormatSpec::Fixed(decimals) => format!("{:.*}", decimals as usize, value),
            FormatSpec::Percent(decimals) => format_percent(value, decimals),
            FormatSpec::Millis(decimals) => format_millis(value, decimals),
        }
    }
}

impl Column for RedisInfoFieldColumn {
    fn header(&self) -> &str {
        &self.header
    }

    fn align(&self) -> Align {
        self.align
    }

    fn width_hint(&self) -> WidthHint {
        self.width_hint
    }

    fn render_cell(&self, ctx: &RenderCtx<'_>) -> CellText {
        let text = self
            .format_value(ctx.snap)
            .unwrap_or_else(|| self.missing.clone());
        CellText::plain(text)
    }

    fn sort_key(&self, ctx: &SortCtx<'_>) -> SortKey {
        match self.value_type {
            ValueType::String => parse_string(ctx.snap, &self.info_key)
                .map(|v| SortKey::Str(v.to_ascii_lowercase()))
                .unwrap_or(SortKey::Null),
            ValueType::U64 | ValueType::Bytes => parse_u64(ctx.snap, &self.info_key)
                .map(SortKey::U64)
                .unwrap_or(SortKey::Null),
            ValueType::I64 => parse_i64(ctx.snap, &self.info_key)
                .map(SortKey::I64)
                .unwrap_or(SortKey::Null),
            ValueType::F64 | ValueType::Percent => parse_f64(ctx.snap, &self.info_key)
                .map(SortKey::F64)
                .unwrap_or(SortKey::Null),
            ValueType::Bool => parse_bool(ctx.snap, &self.info_key)
                .map(SortKey::Bool)
                .unwrap_or(SortKey::Null),
        }
    }
}
