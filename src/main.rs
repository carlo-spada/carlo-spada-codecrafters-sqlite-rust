use anyhow::{bail, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

const HEADER_SIZE: usize = 100;
const PAGE_HEADER_SIZE: usize = 8;
const MAGIC_PREFIX: &[u8; 16] = b"SQLite format 3\0";

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

struct SelectParts {
    targets: Vec<String>, // one or more column names, or ["COUNT(*)"]
    table:   String,
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
    let targets: Vec<String> = targets_joined
        .split(',')
        .map(|s| s.trim().trim_end_matches(';').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if table.is_empty() || targets.is_empty() {
        return None;
    }

    Some(SelectParts { targets, table })
}

fn handle_select(select: SelectParts, db_path: &str) -> Result<()> {
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

        if tbl_name.eq_ignore_ascii_case(select.table.as_str()) {
            rootpage = Some(rp);
            create_sql = Some(sql);
            break;
        }
    }

    let rootpage = match rootpage {
        Some(rp) => rp as usize,
        None => {
            if select.targets.len() == 1 && select.targets[0].eq_ignore_ascii_case("COUNT(*)") {
                println!("0");
            }
            return Ok(());
        }
    };

    if select.targets.len() == 1 && select.targets[0].eq_ignore_ascii_case("COUNT(*)") {
        let page_start = (rootpage - 1) * page_size;
        let mut buf = vec![0u8; page_size];
        file.seek(SeekFrom::Start(page_start as u64))?;
        file.read_exact(&mut buf)?;

        let ph_off = if rootpage == 1 { HEADER_SIZE } else { 0 };
        let row_count = u16::from_be_bytes([buf[ph_off + 3], buf[ph_off + 4]]) as u64;
        println!("{}", row_count);
        return Ok(());
    }

    let create_sql = create_sql.unwrap_or_default();
    let cols = parse_columns_from_create_sql(&create_sql);
    // Map requested targets to column indexes
    let mut target_indexes: Vec<usize> = Vec::with_capacity(select.targets.len());
    for t in &select.targets {
        let idx = cols.iter().position(|c| c.eq_ignore_ascii_case(t))
            .ok_or_else(|| anyhow::anyhow!(format!("column not found: {}", t)))?;
        target_indexes.push(idx);
    }

    let page_start = (rootpage - 1) * page_size;
    let mut buf = vec![0u8; page_size];
    file.seek(SeekFrom::Start(page_start as u64))?;
    file.read_exact(&mut buf)?;

    let ph_off = if rootpage == 1 { HEADER_SIZE } else { 0 };
    let row_cells = u16::from_be_bytes([buf[ph_off + 3], buf[ph_off + 4]]) as usize;
    let ptr_base = ph_off + PAGE_HEADER_SIZE;

    let mut out_lines: Vec<String> = Vec::with_capacity(row_cells);

    for i in 0..row_cells {
        let off_idx = ptr_base + i * 2;
        let cell_off = u16::from_be_bytes([buf[off_idx], buf[off_idx + 1]]) as usize;
        let mut idx = cell_off;

        let (_, n1) = read_varint(&buf, idx);
        idx = n1;
        let (_, n2) = read_varint(&buf, idx);
        idx = n2;

        let (hdr_sz, hdr_next) = read_varint(&buf, idx);
        let hdr_sz = hdr_sz as usize;
        let mut cur = hdr_next;
        let mut consumed = cur - idx;
        let mut serials: Vec<u64> = Vec::with_capacity(cols.len());
        while consumed < hdr_sz {
            let (code, nxt) = read_varint(&buf, cur);
            serials.push(code);
            consumed += nxt - cur;
            cur = nxt;
        }
        let body_start = cur;

        // Precompute offsets for each column within the record body
        let mut offsets: Vec<usize> = Vec::with_capacity(serials.len());
        let mut acc = 0usize;
        for s in &serials {
            offsets.push(acc);
            acc += serial_type_size(*s);
        }

        // Collect requested columns
        let mut row_fields: Vec<String> = Vec::with_capacity(target_indexes.len());
        for &tidx in &target_indexes {
            let start = body_start + offsets[tidx];
            let len = serial_type_size(serials[tidx]);
            let bytes = &buf[start..start + len];
            // For this stage we expect TEXT columns
            let v = std::str::from_utf8(bytes)?.to_owned();
            row_fields.push(v);
        }
        out_lines.push(row_fields.join("|"));
    }

    for line in out_lines {
        println!("{}", line);
    }

    Ok(())
}

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

fn parse_columns_from_create_sql(sql: &str) -> Vec<String> {
    // Very naive: find the first '(' ... last ')' and split by commas at the top level.
    // Then take the first token of each segment as the column name, skipping table constraints.
    let mut cols = Vec::new();
    let open = match sql.find('(') { Some(i) => i, None => return cols };
    let close = match sql.rfind(')') { Some(i) if i > open => i, _ => return cols };
    let inner = &sql[open+1..close];
    for raw in inner.split(',') {
        let part = raw.trim();
        if part.is_empty() { continue; }
        // first token is candidate column name
        let mut it = part.split_whitespace();
        if let Some(tok) = it.next() {
            let lower = tok.trim_matches(|c: char| c == '"' || c == '`' || c == '[' || c == ']').to_lowercase();
            // skip common table constraints
            match lower.as_str() {
                "primary" | "foreign" | "unique" | "check" | "constraint" => continue,
                _ => cols.push(lower),
            }
        }
    }
    cols
}
