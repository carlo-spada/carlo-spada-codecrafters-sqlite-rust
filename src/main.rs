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
        cmd if cmd.to_uppercase().starts_with("SELECT") => {
            // Very naive parser per stage instructions:
            // Expect: SELECT COUNT(*) FROM <table>
            let tokens: Vec<&str> = command.split_whitespace().collect();
            let table = tokens
                .last()
                .map(|t| t.trim_end_matches(';'))
                .unwrap_or("");
            if table.is_empty() {
                bail!("Could not parse table name");
            }

            let (mut file, header) = open_db(&args[1])?;
            let page_size = page_size_from_header(&header);
            let page1 = read_first_page(&mut file, &header, page_size)?;

            let cell_count =
                u16::from_be_bytes([page1[HEADER_SIZE + 3], page1[HEADER_SIZE + 4]]) as usize;
            let pointer_start = HEADER_SIZE + PAGE_HEADER_SIZE;
            let pointer_bytes = &page1[pointer_start..pointer_start + cell_count * 2];

            let mut rootpage: Option<u64> = None;
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
                let (col3_serial, _) = read_varint(&page1, next);

                let mut body_idx = header_end;
                body_idx += serial_type_size(col0_serial);
                body_idx += serial_type_size(col1_serial);

                let tbl_len = serial_type_size(col2_serial);
                let tbl_bytes = &page1[body_idx..body_idx + tbl_len];
                let tbl_name = std::str::from_utf8(tbl_bytes)?;
                body_idx += tbl_len;

                let root_len = serial_type_size(col3_serial);
                let root_bytes = &page1[body_idx..body_idx + root_len];
                let root = read_signed_be_int(root_bytes) as u64;

                if tbl_name == table {
                    rootpage = Some(root);
                    break;
                }
            }

            let rootpage = match rootpage {
                Some(rp) => rp as usize,
                None => {
                    println!("0");
                    return Ok(());
                }
            };

            let page_start = (rootpage - 1) * page_size;
            let mut buf = vec![0u8; page_size];
            file.seek(SeekFrom::Start(page_start as u64))?;
            file.read_exact(&mut buf)?;

            let ph_off = if rootpage == 1 { HEADER_SIZE } else { 0 };
            let row_count = u16::from_be_bytes([buf[ph_off + 3], buf[ph_off + 4]]) as u64;

            println!("{}", row_count);
            return Ok(());
        }
        _ => bail!("Missing or invalid command passed: {}", command),
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
