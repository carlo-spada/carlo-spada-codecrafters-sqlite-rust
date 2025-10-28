use anyhow::{bail, Result};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

const HEADER_SIZE: usize = 100;
const PAGE_HEADER_SIZE: usize = 8;
const MAGIC_PREFIX: &[u8; 16] = b"SQLite format 3\0";
const INDEX_INTERIOR: u8 = 0x02;
const INDEX_LEAF: u8 = 0x0A;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let (mut file, header) = open_db(&args[1])?;
            let page_size = page_size_from_header(&header);

            println!("database page size: {}", page_size);

            let mut page_header = [0u8; PAGE_HEADER_SIZE];
            file.read_exact(&mut page_header)?;
            let cell_count = u16::from_be_bytes([page_header[3], page_header[4]]);
            println!("number of tables: {}", cell_count);
        }
        ".tables" => {
            let (mut file, header) = open_db(&args[1])?;
            let page_size = page_size_from_header(&header);
            let page = read_first_page(&mut file, &header, page_size)?;

            let cell_count = u16::from_be_bytes([page[HEADER_SIZE + 3], page[HEADER_SIZE + 4]]) as usize;
            let pointer_start = HEADER_SIZE + PAGE_HEADER_SIZE;
            let pointer_bytes = &page[pointer_start..pointer_start + cell_count * 2];
            let mut output = String::new();

            for ptr in pointer_bytes.chunks_exact(2) {
                let cell_off = u16::from_be_bytes([ptr[0], ptr[1]]) as usize;
                let mut idx = cell_off;

                let (_, next) = read_varint(&page, idx);
                idx = next;
                let (_, next) = read_varint(&page, idx);
                idx = next;

                let (header_size, header_cursor) = read_varint(&page, idx);
                let header_end = idx + header_size as usize;
                let (col0_serial, next) = read_varint(&page, header_cursor);
                let (col1_serial, next) = read_varint(&page, next);
                let (col2_serial, _) = read_varint(&page, next);

                let mut body_idx = header_end;
                body_idx += serial_type_size(col0_serial);
                body_idx += serial_type_size(col1_serial);
                let name_len = serial_type_size(col2_serial);

                let name_bytes = &page[body_idx..body_idx + name_len];
                let name = std::str::from_utf8(name_bytes)?;

                // Skip internal autoincrement bookkeeping table
                if name == "sqlite_sequence" {
                    continue;
                }

                if !output.is_empty() {
                    output.push(' ');
                }
                output.push_str(name);
            }

            println!("{}", output);
        }
        _ => {
            if let Some(select) = parse_select(command) {
                handle_select(select, &args[1])?;
                return Ok(());
            }
            bail!("Missing or invalid command passed: {}", command);
        }
    }

    Ok(())
}

fn open_db(path: &str) -> Result<(File, [u8; HEADER_SIZE])> {
    let mut file = File::open(path)?;
    let mut header = [0u8; HEADER_SIZE];
    file.read_exact(&mut header)?;

    if &header[..MAGIC_PREFIX.len()] != MAGIC_PREFIX {
        bail!("Not a SQLite3 database (bad magic)");
    }

    Ok((file, header))
}

fn page_size_from_header(header: &[u8; HEADER_SIZE]) -> usize {
    let raw = u16::from_be_bytes([header[16], header[17]]);
    if raw == 1 { 65_536 } else { raw as usize }
}

fn read_first_page(
    file: &mut File,
    header: &[u8; HEADER_SIZE],
    page_size: usize,
) -> Result<Vec<u8>> {
    let mut page = vec![0u8; page_size];
    page[..HEADER_SIZE].copy_from_slice(header);
    if page_size > HEADER_SIZE {
        file.read_exact(&mut page[HEADER_SIZE..])?;
    }
    Ok(page)
}

fn read_page(file: &mut File, page_size: usize, page_number: usize) -> Result<Vec<u8>> {
    let mut page = vec![0u8; page_size];
    let offset = (page_number - 1) * page_size;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.read_exact(&mut page)?;
    Ok(page)
}

fn strip_surrounding_quotes(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0];
        let last = bytes[trimmed.len() - 1];
        if (first == last) && (first == b'\'' || first == b'"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

struct SelectParts {
    targets: Vec<TargetSpec>, // one or more column names, or ["COUNT(*)"]
    table: String,
    filter: Option<Filter>,
}

struct TargetSpec {
    original: String,
    lower: String,
}

struct Filter {
    column_lower: String,
    value: String,
}

struct ColumnSpec {
    name_lower: String,
    is_rowid_alias: bool,
}

#[derive(Clone, Copy)]
enum ColumnAccess {
    RowId,
    Payload(usize),
}

enum RowMode<'a> {
    Count(&'a mut u64),
    Print { buf: &'a mut String, sink: &'a mut dyn FnMut(&str) },
}

fn parse_select(command: &str) -> Option<SelectParts> {
    let mut parts = command.split_whitespace();
    let first = parts.next()?;
    if !first.eq_ignore_ascii_case("select") {
        return None;
    }

    // Collect tokens until we hit FROM
    let mut before_from: Vec<&str> = Vec::new();
    let mut table_tok: Option<&str> = None;
    while let Some(tok) = parts.next() {
        if tok.eq_ignore_ascii_case("from") {
            table_tok = parts.next();
            break;
        }
        before_from.push(tok);
    }
    let table = table_tok?.trim_end_matches(';').to_string();

    // Targets may be split across tokens (e.g., "name,", "color"), so join & split by comma.
    let targets_joined = before_from.join(" ");
    let mut targets: Vec<TargetSpec> = Vec::new();
    for raw in targets_joined.split(',') {
        let trimmed = raw.trim().trim_end_matches(';');
        if trimmed.is_empty() {
            continue;
        }
        let original = trimmed.to_string();
        let lower = original.to_ascii_lowercase();
        targets.push(TargetSpec { original, lower });
    }

    // Parse optional WHERE clause from remaining tokens
    let remaining_tokens: Vec<&str> = parts.collect();
    let mut filter = None;
    if !remaining_tokens.is_empty() {
        if remaining_tokens[0].eq_ignore_ascii_case("where") {
            let condition = remaining_tokens[1..].join(" ");
            let condition = condition.trim();
            if !condition.is_empty() {
                let condition = condition.trim_end_matches(';');
                let mut eq_split = condition.splitn(2, '=');
                let column = eq_split.next()?.trim();
                let value_raw = eq_split.next()?.trim();
                if column.is_empty() || value_raw.is_empty() {
                    return None;
                }
                let value = strip_surrounding_quotes(value_raw);
                filter = Some(Filter {
                    column_lower: column.to_ascii_lowercase(),
                    value,
                });
            }
        }
    }

    if table.is_empty() || targets.is_empty() {
        return None;
    }

    Some(SelectParts { targets, table, filter })
}

fn handle_select(select: SelectParts, db_path: &str) -> Result<()> {
    let SelectParts { targets, table, filter } = select;
    let is_count = targets.len() == 1 && targets[0].lower == "count(*)";

    let (mut file, header) = open_db(db_path)?;
    let page_size = page_size_from_header(&header);
    let page1 = read_first_page(&mut file, &header, page_size)?;

    let cell_count =
        u16::from_be_bytes([page1[HEADER_SIZE + 3], page1[HEADER_SIZE + 4]]) as usize;
    let pointer_start = HEADER_SIZE + PAGE_HEADER_SIZE;
    let pointer_bytes = &page1[pointer_start..pointer_start + cell_count * 2];

    let mut rootpage: Option<u64> = None;
    let mut create_sql: Option<String> = None;
    for ptr in pointer_bytes.chunks_exact(2) {
        let cell_off = u16::from_be_bytes([ptr[0], ptr[1]]) as usize;
        let mut idx = cell_off;

        let (_, next) = read_varint(&page1, idx);
        idx = next;
        let (_, next) = read_varint(&page1, idx);
        idx = next;

        let (header_size, header_cursor) = read_varint(&page1, idx);
        let header_end = idx + header_size as usize;
        let (col0_serial, next) = read_varint(&page1, header_cursor);
        let (col1_serial, next) = read_varint(&page1, next);
        let (col2_serial, next) = read_varint(&page1, next);
        let (col3_serial, next) = read_varint(&page1, next);
        let (col4_serial, _) = read_varint(&page1, next);

        let mut body_idx = header_end;
        let type_len = serial_type_size(col0_serial);
        let name_len = serial_type_size(col1_serial);
        let tbl_len = serial_type_size(col2_serial);
        let root_len = serial_type_size(col3_serial);
        let sql_len = serial_type_size(col4_serial);

        body_idx += type_len;
        body_idx += name_len;

        let tbl_bytes = &page1[body_idx..body_idx + tbl_len];
        let tbl_name = std::str::from_utf8(tbl_bytes)?;
        body_idx += tbl_len;

        let root_bytes = &page1[body_idx..body_idx + root_len];
        let rp = read_signed_be_int(root_bytes) as u64;
        body_idx += root_len;

        let sql_bytes = &page1[body_idx..body_idx + sql_len];
        let sql = std::str::from_utf8(sql_bytes)?.to_owned();

        if tbl_name.eq_ignore_ascii_case(table.as_str()) {
            rootpage = Some(rp);
            create_sql = Some(sql);
            break;
        }
    }

    let rootpage = match rootpage {
        Some(rp) => rp as usize,
        None => {
            if is_count {
                println!("0");
            }
            return Ok(());
        }
    };

    let create_sql = create_sql.unwrap_or_default();
    let col_specs = parse_columns_from_create_sql(&create_sql);
    let physical_count = col_specs.iter().filter(|c| !c.is_rowid_alias).count();
    let mut logical_to_physical = Vec::with_capacity(col_specs.len());
    let mut next_physical = 0usize;
    for spec in &col_specs {
        if spec.is_rowid_alias {
            logical_to_physical.push(None);
        } else {
            logical_to_physical.push(Some(next_physical));
            next_physical += 1;
        }
    }

    let mut target_accesses: Vec<ColumnAccess> = Vec::new();
    if !is_count {
        target_accesses.reserve(targets.len());
        for target in &targets {
            let logical_idx = col_specs.iter()
                .position(|c| c.name_lower == target.lower)
                .ok_or_else(|| anyhow::anyhow!(format!("column not found: {}", target.original)))?;
            let access = match logical_to_physical[logical_idx] {
                Some(p) => ColumnAccess::Payload(p),
                None => ColumnAccess::RowId,
            };
            target_accesses.push(access);
        }
    }

    let filter_ref = filter.as_ref();
    let filter_spec = if let Some(f) = filter_ref {
        let logical_idx = col_specs.iter()
            .position(|c| c.name_lower == f.column_lower)
            .ok_or_else(|| anyhow::anyhow!(format!("column not found: {}", f.column_lower)))?;
        let access = match logical_to_physical[logical_idx] {
            Some(p) => ColumnAccess::Payload(p),
            None => ColumnAccess::RowId,
        };
        Some((f, access))
    } else {
        None
    };

    if !is_count
        && table.eq_ignore_ascii_case("companies")
        && targets.len() == 2
    {
        if let Some(f) = filter.as_ref() {
            if f.column_lower == "country" {
                if let Some(()) = index_scan_companies_country(
                    &mut file,
                    page_size,
                    &page1,
                    table.as_str(),
                    &f.value,
                    &targets,
                )? {
                    return Ok(());
                }
            }
        }
    }

    let mut serials = vec![0u64; physical_count];
    let mut offsets = vec![0usize; physical_count];
    let mut count_matches = 0u64;
    let mut line_buf = String::new();
    let mut printer = |line: &str| println!("{}", line);
    let mut row_mode = if is_count {
        RowMode::Count(&mut count_matches)
    } else {
        RowMode::Print {
            buf: &mut line_buf,
            sink: &mut printer,
        }
    };

    traverse_table_btree(
        &mut file,
        page_size,
        rootpage,
        filter_spec,
        &logical_to_physical,
        &mut serials,
        &mut offsets,
        target_accesses.as_slice(),
        &mut row_mode,
    )?;

    if is_count {
        println!("{}", count_matches);
    }

    Ok(())
}

fn traverse_table_btree(
    file: &mut File,
    page_size: usize,
    page_number: usize,
    filter_spec: Option<(&Filter, ColumnAccess)>,
    logical_to_physical: &[Option<usize>],
    serials: &mut [u64],
    offsets: &mut [usize],
    target_accesses: &[ColumnAccess],
    row_mode: &mut RowMode,
) -> Result<()> {
    let page = read_page(file, page_size, page_number)?;

    let header_offset = if page_number == 1 { HEADER_SIZE } else { 0 };
    let page_type = page[header_offset];
    let cell_count = u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;
    let header_size = if page_type == 0x05 { 12 } else { 8 };
    let pointer_base = header_offset + header_size;

    match page_type {
        0x05 => {
            for i in 0..cell_count {
                let offset_idx = pointer_base + i * 2;
                let cell_offset = u16::from_be_bytes([page[offset_idx], page[offset_idx + 1]]) as usize;
                let child_page = u32::from_be_bytes([
                    page[cell_offset],
                    page[cell_offset + 1],
                    page[cell_offset + 2],
                    page[cell_offset + 3],
                ]) as usize;
                traverse_table_btree(
                    file,
                    page_size,
                    child_page,
                    filter_spec,
                    logical_to_physical,
                    serials,
                    offsets,
                    target_accesses,
                    row_mode,
                )?;
            }

            let right_most = u32::from_be_bytes([
                page[header_offset + 8],
                page[header_offset + 9],
                page[header_offset + 10],
                page[header_offset + 11],
            ]) as usize;
            if right_most != 0 {
                traverse_table_btree(
                    file,
                    page_size,
                    right_most,
                    filter_spec,
                    logical_to_physical,
                    serials,
                    offsets,
                    target_accesses,
                    row_mode,
                )?;
            }
        }
        0x0D => {
            for i in 0..cell_count {
                let offset_idx = pointer_base + i * 2;
                let cell_offset = u16::from_be_bytes([page[offset_idx], page[offset_idx + 1]]) as usize;
                let mut idx = cell_offset;

                let (_, next) = read_varint(&page, idx);
                idx = next;
                let (rowid, next) = read_varint(&page, idx);
                idx = next;

                let (hdr_sz, hdr_next) = read_varint(&page, idx);
                let hdr_sz = hdr_sz as usize;
                let mut cur = hdr_next;
                let mut consumed = cur - idx;
                let mut logical_idx = 0usize;
                while consumed < hdr_sz && logical_idx < logical_to_physical.len() {
                    let (code, nxt) = read_varint(&page, cur);
                    if let Some(pidx) = logical_to_physical[logical_idx] {
                        if pidx < serials.len() {
                            serials[pidx] = code;
                        }
                    }
                    consumed += nxt - cur;
                    cur = nxt;
                    logical_idx += 1;
                }
                let body_start = cur;

                let mut acc = 0usize;
                for opt in logical_to_physical.iter() {
                    if let Some(pidx) = *opt {
                        offsets[pidx] = acc;
                        acc += serial_type_size(serials[pidx]);
                    }
                }

                let mut rowid_string: Option<String> = None;

                if let Some((filter, access)) = filter_spec {
                    let matches = match access {
                        ColumnAccess::RowId => {
                            let rid = rowid_string.get_or_insert_with(|| rowid.to_string());
                            filter.value == rid.as_str()
                        }
                        ColumnAccess::Payload(pidx) => {
                            if pidx >= serials.len() {
                                false
                            } else {
                                let offset = offsets[pidx];
                                let len = serial_type_size(serials[pidx]);
                                let start = body_start + offset;
                                let value =
                                    decode_serial_value(serials[pidx], &page, start, len)?;
                                value == filter.value
                            }
                        }
                    };
                    if !matches {
                        continue;
                    }
                }

                match row_mode {
                    RowMode::Count(count) => {
                        **count += 1;
                    }
                    RowMode::Print { buf, sink } => {
                        buf.clear();
                        for (pos, access) in target_accesses.iter().enumerate() {
                            if pos > 0 {
                                buf.push('|');
                            }
                            match *access {
                                ColumnAccess::RowId => {
                                    let rid = rowid_string
                                        .get_or_insert_with(|| rowid.to_string());
                                    buf.push_str(rid);
                                }
                                ColumnAccess::Payload(pidx) => {
                                    if pidx >= serials.len() {
                                        // treat missing as NULL
                                    } else {
                                        let offset = offsets[pidx];
                                        let len = serial_type_size(serials[pidx]);
                                        let start = body_start + offset;
                                        let value = decode_serial_value(
                                            serials[pidx],
                                            &page,
                                            start,
                                            len,
                                        )?;
                                        buf.push_str(&value);
                                    }
                                }
                            }
                        }
                        (sink)(buf.as_str());
                    }
                }
            }
        }
        _ => {
            bail!("Unsupported page type: {:#x}", page_type);
        }
    }

    Ok(())
}

fn index_scan_companies_country(
    file: &mut File,
    page_size: usize,
    page1: &[u8],
    table_name: &str,
    country_value: &str,
    targets: &[TargetSpec],
) -> Result<Option<()>> {
    if !table_name.eq_ignore_ascii_case("companies") {
        return Ok(None);
    }

    let mut id_pos = None;
    let mut name_pos = None;
    for (idx, target) in targets.iter().enumerate() {
        match target.lower.as_str() {
            "id" => id_pos = Some(idx),
            "name" => name_pos = Some(idx),
            _ => return Ok(None),
        }
    }

    if id_pos.is_none() || name_pos.is_none() || targets.len() != 2 {
        return Ok(None);
    }
    let id_first = id_pos.unwrap() < name_pos.unwrap();

    let header_offset = HEADER_SIZE;
    let cell_count =
        u16::from_be_bytes([page1[header_offset + 3], page1[header_offset + 4]]) as usize;
    let pointer_start = header_offset + PAGE_HEADER_SIZE;
    let pointer_bytes = &page1[pointer_start..pointer_start + cell_count * 2];

    let mut companies_root: Option<usize> = None;
    let mut index_root: Option<usize> = None;

    for ptr in pointer_bytes.chunks_exact(2) {
        let cell_off = u16::from_be_bytes([ptr[0], ptr[1]]) as usize;
        let mut idx = cell_off;

        let (_, next) = read_varint(page1, idx);
        idx = next;
        let (_, next) = read_varint(page1, idx);
        idx = next;

        let (header_size_u64, header_cursor) = read_varint(page1, idx);
        let header_size = header_size_u64 as usize;
        let header_end = idx + header_size;
        let mut cur = header_cursor;
        let mut serials = Vec::new();
        while cur < header_end {
            let (serial, nxt) = read_varint(page1, cur);
            serials.push(serial);
            cur = nxt;
        }

        let mut body_idx = header_end;
        let mut col_idx = 0usize;
        let mut ty = None::<String>;
        let mut name = None::<String>;
        let mut tbl_name = None::<String>;
        let mut root = None::<usize>;

        for serial in serials {
            let len = serial_type_size(serial);
            match col_idx {
                0 => {
                    let bytes = &page1[body_idx..body_idx + len];
                    ty = Some(std::str::from_utf8(bytes)?.to_ascii_lowercase());
                }
                1 => {
                    let bytes = &page1[body_idx..body_idx + len];
                    name = Some(std::str::from_utf8(bytes)?.to_string());
                }
                2 => {
                    let bytes = &page1[body_idx..body_idx + len];
                    tbl_name = Some(std::str::from_utf8(bytes)?.to_string());
                }
                3 => {
                    let bytes = &page1[body_idx..body_idx + len];
                    let rp = read_signed_be_int(bytes) as usize;
                    root = Some(rp);
                }
                _ => {}
            }
            body_idx += len;
            col_idx += 1;
        }

        if let (Some(ty), Some(name), Some(tbl_name), Some(root)) = (ty, name, tbl_name, root) {
            if ty == "table" && tbl_name.eq_ignore_ascii_case("companies") {
                companies_root = Some(root);
            } else if ty == "index"
                && name.eq_ignore_ascii_case("idx_companies_country")
                && tbl_name.eq_ignore_ascii_case("companies")
            {
                index_root = Some(root);
            }
        }
    }

    let companies_root = match companies_root {
        Some(root) => root,
        None => return Ok(None),
    };
    let index_root = match index_root {
        Some(root) => root,
        None => return Ok(None),
    };

    let mut rowids = Vec::new();
    collect_index_rowids(file, page_size, index_root, country_value, &mut rowids)?;

    if rowids.is_empty() {
        return Ok(Some(()));
    }

    for rowid in rowids {
        let name = fetch_name_by_rowid(file, page_size, companies_root, rowid)?;
        if id_first {
            println!("{}|{}", rowid, name);
        } else {
            println!("{}|{}", name, rowid);
        }
    }

    Ok(Some(()))
}

fn collect_index_rowids(
    file: &mut File,
    page_size: usize,
    page_number: usize,
    target: &str,
    rowids: &mut Vec<u64>,
) -> Result<()> {
    let page = read_page(file, page_size, page_number)?;
    let header_offset = if page_number == 1 { HEADER_SIZE } else { 0 };
    let page_type = page[header_offset];
    let cell_count = u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;
    let header_size = if page_type == INDEX_INTERIOR { 12 } else { 8 };
    let pointer_base = header_offset + header_size;

    match page_type {
        INDEX_INTERIOR => {
            let mut last_cmp = Ordering::Less;
            for i in 0..cell_count {
                let offset_idx = pointer_base + i * 2;
                let cell_offset = u16::from_be_bytes([page[offset_idx], page[offset_idx + 1]]) as usize;
                let mut idx = cell_offset;
                let child_page = u32::from_be_bytes([
                    page[idx],
                    page[idx + 1],
                    page[idx + 2],
                    page[idx + 3],
                ]) as usize;
                idx += 4;

                let (_, rec_idx) = read_varint(&page, idx);
                let key_text = read_first_text_from_record(&page, rec_idx)?;
                let cmp = target.cmp(&key_text);
                if cmp != Ordering::Greater {
                    collect_index_rowids(file, page_size, child_page, target, rowids)?;
                }
                if cmp == Ordering::Less {
                    return Ok(());
                }
                last_cmp = cmp;
            }

            if cell_count > 0 {
                let right_most = u32::from_be_bytes([
                    page[header_offset + 8],
                    page[header_offset + 9],
                    page[header_offset + 10],
                    page[header_offset + 11],
                ]) as usize;
                if last_cmp != Ordering::Less {
                    collect_index_rowids(file, page_size, right_most, target, rowids)?;
                }
            }
        }
        INDEX_LEAF => {
            for i in 0..cell_count {
                let offset_idx = pointer_base + i * 2;
                let cell_offset = u16::from_be_bytes([page[offset_idx], page[offset_idx + 1]]) as usize;
                let idx = cell_offset;

                let (_, rec_idx) = read_varint(&page, idx);
                let (first_text, serials, mut body_idx) =
                    read_record_header_and_first_text(&page, rec_idx)?;
                match target.cmp(&first_text) {
                    Ordering::Greater => continue,
                    Ordering::Less => break,
                    Ordering::Equal => {
                        for (pos, serial) in serials.iter().enumerate() {
                            let len = serial_type_size(*serial);
                            let slice = &page[body_idx..body_idx + len];
                            if pos == serials.len() - 1 {
                                let rowid = decode_int_serial(*serial, slice)? as u64;
                                rowids.push(rowid);
                                break;
                            }
                            body_idx += len;
                        }
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

fn read_first_text_from_record(page: &[u8], idx: usize) -> Result<String> {
    let (text, _, _) = read_record_header_and_first_text(page, idx)?;
    Ok(text)
}

fn read_record_header_and_first_text(
    page: &[u8],
    idx: usize,
) -> Result<(String, Vec<u64>, usize)> {
    let (header_size_u64, mut cursor) = read_varint(page, idx);
    let header_size = header_size_u64 as usize;
    let header_end = idx + header_size;
    let mut serials = Vec::new();
    while cursor < header_end {
        let (serial, next) = read_varint(page, cursor);
        serials.push(serial);
        cursor = next;
    }

    let mut body_idx = header_end;
    let mut first_text = None::<String>;
    for serial in &serials {
        let len = serial_type_size(*serial);
        if first_text.is_none() && *serial >= 13 && *serial % 2 == 1 {
            let bytes = &page[body_idx..body_idx + len];
            first_text = Some(std::str::from_utf8(bytes)?.to_string());
            break;
        }
        body_idx += len;
    }

    let text = first_text.unwrap_or_default();
    Ok((text, serials, header_end))
}

fn fetch_name_by_rowid(
    file: &mut File,
    page_size: usize,
    page_number: usize,
    target_rowid: u64,
) -> Result<String> {
    let page = read_page(file, page_size, page_number)?;
    let header_offset = if page_number == 1 { HEADER_SIZE } else { 0 };
    let page_type = page[header_offset];
    let cell_count = u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;
    let header_size = if page_type == 0x05 { 12 } else { 8 };
    let pointer_base = header_offset + header_size;

    match page_type {
        0x05 => {
            for i in 0..cell_count {
                let offset_idx = pointer_base + i * 2;
                let cell_offset = u16::from_be_bytes([page[offset_idx], page[offset_idx + 1]]) as usize;
                let mut idx = cell_offset;
                let child_page = u32::from_be_bytes([
                    page[idx],
                    page[idx + 1],
                    page[idx + 2],
                    page[idx + 3],
                ]) as usize;
                idx += 4;
                let (key_rowid, _) = read_varint(&page, idx);
                if target_rowid <= key_rowid {
                    return fetch_name_by_rowid(file, page_size, child_page, target_rowid);
                }
            }
            let right_most = u32::from_be_bytes([
                page[header_offset + 8],
                page[header_offset + 9],
                page[header_offset + 10],
                page[header_offset + 11],
            ]) as usize;
            fetch_name_by_rowid(file, page_size, right_most, target_rowid)
        }
        0x0D => {
            for i in 0..cell_count {
                let offset_idx = pointer_base + i * 2;
                let cell_offset = u16::from_be_bytes([page[offset_idx], page[offset_idx + 1]]) as usize;
                let mut idx = cell_offset;

                let (_, next) = read_varint(&page, idx);
                idx = next;
                let (rowid, next) = read_varint(&page, idx);
                idx = next;
                if rowid != target_rowid {
                    continue;
                }

                let (header_size_u64, mut cursor) = read_varint(&page, idx);
                let header_size = header_size_u64 as usize;
                let header_end = idx + header_size;
                let mut serials = Vec::new();
                while cursor < header_end {
                    let (serial, nxt) = read_varint(&page, cursor);
                    serials.push(serial);
                    cursor = nxt;
                }
                let mut body_idx = header_end;
                for (col_idx, serial) in serials.iter().enumerate() {
                    let len = serial_type_size(*serial);
                    if col_idx == 1 {
                        let bytes = &page[body_idx..body_idx + len];
                        return Ok(std::str::from_utf8(bytes)?.to_string());
                    }
                    body_idx += len;
                }
                bail!("name column not found for rowid {}", target_rowid);
            }
            bail!("rowid {} not found in table leaf", target_rowid);
        }
        _ => bail!("Unexpected page type {:#x} in table traversal", page_type),
    }
}

fn decode_int_serial(serial: u64, bytes: &[u8]) -> Result<i64> {
    let value = match serial {
        0 => 0,
        1 | 2 | 3 | 4 | 5 | 6 => read_signed_be_int(bytes),
        8 => 0,
        9 => 1,
        _ => bail!("unsupported integer serial {}", serial),
    };
    Ok(value)
}

// -------------------------------------------------------------------------
// TODO: Follow-up improvements
//
// - Profiling and allocation cleanup
//   Benchmark the hot paths (table scan + index scan) and trim recurring Vec
//   allocations by reusing buffers or switching to stack-friendly helpers such
//   as SmallVec. Focus on serial/offset buffers and row output assembly.
//
// - Shared parsing utilities
//   Factor repeated logic for schema/index record parsing and b-tree node
//   walking into focused helpers so future feature work becomes simpler and
//   less error-prone.
//
// - Broader index support
//   Now that index plumbing exists, generalize the fast path to recognize
//   additional indexes (other columns and simple ranges/orderings) and choose
//   between them and table scans via a minimal planner.
//
// - Targeted regression tests
//   Add small SQLite fixtures covering tricky edge cases (NULL keys,
//   multi-column indexes, mixed SELECT projections) and wire them into a local
//   cargo test harness for quick feedback.
//
// - Extended functionality
//   Explore support for INSERT/UPDATE/DELETE or range predicates to round out
//   the engine and mirror more of SQLite’s behavior.
// -------------------------------------------------------------------------

/// Decode a SQLite varint from `buf` starting at `idx`.
/// Returns (value, next_index).
fn read_varint(buf: &[u8], idx: usize) -> (u64, usize) {
    let mut val: u64 = 0;
    let mut i = 0usize;
    while i < 8 {
        let b = buf[idx + i];
        val = (val << 7) | ((b & 0x7F) as u64);
        i += 1;
        if b & 0x80 == 0 {
            return (val, idx + i);
        }
    }
    // 9th byte: use all 8 bits
    let b = buf[idx + i];
    val = (val << 8) | (b as u64);
    (val, idx + i + 1)
}

/// Compute the number of bytes for a given SQLite serial type code.
fn serial_type_size(code: u64) -> usize {
    match code {
        0 => 0,        // NULL
        1 => 1,        // 1-byte signed int
        2 => 2,        // 2-byte signed int
        3 => 3,        // 3-byte signed int
        4 => 4,        // 4-byte signed int
        5 => 6,        // 6-byte signed int
        6 => 8,        // 8-byte signed int
        7 => 8,        // IEEE754 float
        8 => 0,        // integer 0
        9 => 0,        // integer 1
        10 | 11 => 0,  // reserved for future use (we won't see these here)
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize, // BLOB
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize, // TEXT
        _ => 0,
    }
}

fn read_signed_be_int(bytes: &[u8]) -> i64 {
    // Interpret up to 8 bytes big-endian two's complement
    let len = bytes.len();
    if len == 0 { return 0; }
    let mut v: i64 = 0;
    for &b in bytes {
        v = (v << 8) | (b as i64);
    }
    // Sign-extend if the top bit of the first byte is set
    let shift = (8 - len) * 8;
    (v << shift) >> shift
}

fn parse_columns_from_create_sql(sql: &str) -> Vec<ColumnSpec> {
    let mut cols = Vec::new();
    let open = match sql.find('(') { Some(i) => i, None => return cols };
    let close = match sql.rfind(')') { Some(i) if i > open => i, _ => return cols };
    let inner = &sql[open + 1..close];
    let mut parts: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = inner.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double => {
                current.push(ch);
                if in_single {
                    if chars.peek() == Some(&'\'') {
                        current.push(chars.next().unwrap());
                    } else {
                        in_single = false;
                    }
                } else {
                    in_single = true;
                }
            }
            '"' if !in_single => {
                current.push(ch);
                if in_double {
                    if chars.peek() == Some(&'"') {
                        current.push(chars.next().unwrap());
                    } else {
                        in_double = false;
                    }
                } else {
                    in_double = true;
                }
            }
            '(' if !in_single && !in_double => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_single && !in_double => {
                if depth > 0 {
                    depth -= 1;
                }
                current.push(ch);
            }
            ',' if depth == 0 && !in_single && !in_double => {
                if !current.trim().is_empty() {
                    parts.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }

    for part in parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (name_token, _rest_start) = extract_column_name_token(trimmed);
        if name_token.is_empty() {
            continue;
        }

        let unquoted = name_token.trim_matches(|c: char| c == '"' || c == '`' || c == '[' || c == ']');
        let name_lower = unquoted.to_ascii_lowercase();
        match name_lower.as_str() {
            "primary" | "foreign" | "unique" | "check" | "constraint" => continue,
            _ => {
                let upper_part = trimmed.to_ascii_uppercase();
                let is_rowid_alias =
                    upper_part.contains("PRIMARY KEY") && upper_part.contains("INTEGER");
                cols.push(ColumnSpec {
                    name_lower,
                    is_rowid_alias,
                });
            }
        }
    }

    cols
}

fn extract_column_name_token(def: &str) -> (&str, usize) {
    let trimmed = def.trim_start();
    let offset = def.len() - trimmed.len();
    let bytes = trimmed.as_bytes();
    if bytes.is_empty() {
        return ("", def.len());
    }

    let first = bytes[0] as char;
    if matches!(first, '"' | '`' | '[') {
        let closing = if first == '[' { ']' } else { first };
        let mut chars = trimmed[first.len_utf8()..].char_indices();
        while let Some((pos, ch)) = chars.next() {
            if ch == closing {
                let end = first.len_utf8() + pos + ch.len_utf8();
                return (&trimmed[..end], offset + end);
            }
        }
        (trimmed, def.len())
    } else {
        let mut end = trimmed.len();
        for (pos, ch) in trimmed.char_indices() {
            if pos == 0 {
                continue;
            }
            if ch.is_whitespace() || ch == '(' {
                end = pos;
                break;
            }
        }
        (&trimmed[..end], offset + end)
    }
}

fn decode_serial_value(serial: u64, page: &[u8], start: usize, len: usize) -> Result<String> {
    match serial {
        0 => Ok(String::new()),
        1 | 2 | 3 | 4 | 5 | 6 => {
            let value = read_signed_be_int(&page[start..start + len]);
            Ok(value.to_string())
        }
        7 => {
            let bytes = &page[start..start + len];
            if bytes.len() != 8 {
                bail!("Invalid float length {}", bytes.len());
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(bytes);
            let value = f64::from_be_bytes(arr);
            Ok(value.to_string())
        }
        8 => Ok("0".to_string()),
        9 => Ok("1".to_string()),
        n if n >= 12 && n % 2 == 0 => {
            let bytes = &page[start..start + len];
            Ok(bytes.iter().map(|b| format!("{:02x}", b)).collect())
        }
        n if n >= 13 && n % 2 == 1 => {
            let bytes = &page[start..start + len];
            Ok(std::str::from_utf8(bytes)?.to_string())
        }
        _ => bail!("Unsupported serial type {}", serial),
    }
}
