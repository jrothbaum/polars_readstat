use std::collections::HashMap;

/// Shared accumulator for building per-variable metadata DataFrames during file parsing.
///
/// All five formats (SPSS, Stata, SAS, XPT, POR) produce the same 11-column schema.
/// Each parser pushes rows as it reads variable records, updates fields via index-based
/// setters during extension/secondary passes, then calls `into_dataframe()` once at the
/// end of parsing — no separate conversion step needed.
pub struct MetadataAccumulator {
    pub names: Vec<String>,
    labels: Vec<Option<String>>,
    /// Group name each variable belongs to; resolved to codes/labels in into_dataframe().
    var_value_label_names: Vec<Option<String>>,
    pub value_label_groups: HashMap<String, (Vec<String>, Vec<String>)>,
    formats: Vec<Option<String>>,
    format_types: Vec<Option<i32>>,
    format_widths: Vec<Option<i32>>,
    format_decimals: Vec<Option<i32>>,
    measures: Vec<Option<&'static str>>,
    display_widths: Vec<Option<i32>>,
    alignments: Vec<Option<&'static str>>,
}

impl MetadataAccumulator {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            names: Vec::with_capacity(n),
            labels: Vec::with_capacity(n),
            var_value_label_names: Vec::with_capacity(n),
            value_label_groups: HashMap::new(),
            formats: Vec::with_capacity(n),
            format_types: Vec::with_capacity(n),
            format_widths: Vec::with_capacity(n),
            format_decimals: Vec::with_capacity(n),
            measures: Vec::with_capacity(n),
            display_widths: Vec::with_capacity(n),
            alignments: Vec::with_capacity(n),
        }
    }

    /// Append a new variable row.
    pub fn push(
        &mut self,
        name: String,
        label: Option<String>,
        format: Option<String>,
        format_type: Option<i32>,
        format_width: Option<i32>,
        format_decimals: Option<i32>,
    ) {
        self.names.push(name);
        self.labels.push(label);
        self.var_value_label_names.push(None);
        self.formats.push(format);
        self.format_types.push(format_type);
        self.format_widths.push(format_width);
        self.format_decimals.push(format_decimals);
        self.measures.push(None);
        self.display_widths.push(None);
        self.alignments.push(None);
    }

    pub fn set_label(&mut self, idx: usize, label: Option<String>) {
        self.labels[idx] = label;
    }

    pub fn set_measure(&mut self, idx: usize, measure: Option<&'static str>) {
        self.measures[idx] = measure;
    }

    pub fn set_display_width(&mut self, idx: usize, width: Option<i32>) {
        self.display_widths[idx] = width;
    }

    pub fn set_alignment(&mut self, idx: usize, alignment: Option<&'static str>) {
        self.alignments[idx] = alignment;
    }

    pub fn set_value_label_name(&mut self, idx: usize, name: String) {
        self.var_value_label_names[idx] = Some(name);
    }

    pub fn add_value_label_group(&mut self, name: String, codes: Vec<String>, labels: Vec<String>) {
        self.value_label_groups.insert(name, (codes, labels));
    }

    /// Rename variable at `idx` (SPSS long-variable-name extension record).
    pub fn rename(&mut self, idx: usize, new_name: String) {
        self.names[idx] = new_name;
    }

    /// Remove variables in `range` (SPSS very-long-string segment coalescing).
    pub fn drain(&mut self, range: std::ops::Range<usize>) {
        self.names.drain(range.clone());
        self.labels.drain(range.clone());
        self.var_value_label_names.drain(range.clone());
        self.formats.drain(range.clone());
        self.format_types.drain(range.clone());
        self.format_widths.drain(range.clone());
        self.format_decimals.drain(range.clone());
        self.measures.drain(range.clone());
        self.display_widths.drain(range.clone());
        self.alignments.drain(range.clone());
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Re-encode string fields that were originally decoded with `from` encoding.
    /// Called when an SPSS file's encoding extension record overrides the initial assumption.
    pub fn redecode_strings(
        &mut self,
        from: &'static encoding_rs::Encoding,
        to: &'static encoding_rs::Encoding,
    ) {
        for label in &mut self.labels {
            if let Some(s) = label {
                *s = redecode(s, from, to);
            }
        }
        for vl_name in &mut self.var_value_label_names {
            if let Some(s) = vl_name {
                *s = redecode(s, from, to);
            }
        }
        let old_groups: HashMap<String, (Vec<String>, Vec<String>)> =
            std::mem::take(&mut self.value_label_groups);
        self.value_label_groups = old_groups
            .into_iter()
            .map(|(name, (codes, labels))| {
                let name2 = redecode(&name, from, to);
                let codes2 = codes.iter().map(|c| redecode(c, from, to)).collect();
                let labels2 = labels.iter().map(|l| redecode(l, from, to)).collect();
                (name2, (codes2, labels2))
            })
            .collect();
    }

    /// Consume the accumulator and produce the canonical 11-column metadata DataFrame.
    pub fn into_dataframe(self) -> polars::prelude::PolarsResult<polars::prelude::DataFrame> {
        use polars::prelude::*;

        let n = self.names.len();

        // Resolve var_value_label_names → (codes, labels) per variable.
        let mut vl_codes: Vec<Option<Vec<String>>> = Vec::with_capacity(n);
        let mut vl_lbls: Vec<Option<Vec<String>>> = Vec::with_capacity(n);
        for vl_name in &self.var_value_label_names {
            match vl_name.as_ref().and_then(|n| self.value_label_groups.get(n)) {
                Some((codes, labels)) => {
                    vl_codes.push(Some(codes.clone()));
                    vl_lbls.push(Some(labels.clone()));
                }
                None => {
                    vl_codes.push(None);
                    vl_lbls.push(None);
                }
            }
        }

        let list_str_dtype = DataType::List(Box::new(DataType::String));
        let make_list = |col_name: &str,
                         vecs: Vec<Option<Vec<String>>>|
         -> PolarsResult<Series> {
            let any_vals: Vec<AnyValue> = vecs
                .into_iter()
                .map(|opt| match opt {
                    None => AnyValue::Null,
                    Some(v) => AnyValue::List(Series::new(PlSmallStr::EMPTY, v)),
                })
                .collect();
            Series::from_any_values_and_dtype(col_name.into(), &any_vals, &list_str_dtype, true)
        };

        DataFrame::new_infer_height(vec![
            Series::new("name".into(), self.names).into_column(),
            Series::new("label".into(), self.labels).into_column(),
            make_list("value_label_codes", vl_codes)?.into_column(),
            make_list("value_label_labels", vl_lbls)?.into_column(),
            Series::new("format".into(), self.formats).into_column(),
            Series::new("format_type".into(), self.format_types).into_column(),
            Series::new("format_width".into(), self.format_widths).into_column(),
            Series::new("format_decimals".into(), self.format_decimals).into_column(),
            Series::new("measure".into(), self.measures).into_column(),
            Series::new("display_width".into(), self.display_widths).into_column(),
            Series::new("alignment".into(), self.alignments).into_column(),
        ])
    }
}

fn redecode(
    s: &str,
    from: &'static encoding_rs::Encoding,
    to: &'static encoding_rs::Encoding,
) -> String {
    let (bytes, _, _) = from.encode(s);
    to.decode_without_bom_handling(&bytes).0.trim().to_string()
}
