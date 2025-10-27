use anyhow::{bail, Result};
use std::fs::File;
use std::io::Read;

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
            let mut file = File::open(&args[1])?;
            let mut header = [0u8; 100];
            file.read_exact(&mut header)?;

            if &header[0..16] != b"SQLite format 3\0" {
                bail!("Not a SQLite3 database (bad magic)");
            }

            let raw = u16::from_be_bytes([header[16], header[17]]);
            let page_size = if raw == 1 { 65_536 } else { raw as u32 };

            println!("database page size: {}", page_size);

            let mut page_header = [0u8; 8];
            file.read_exact(&mut page_header)?;
            let cell_count = u16::from_be_bytes([page_header[3], page_header[4]]);
            println!("number of tables: {}", cell_count);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0u8; 100];
            file.read_exact(&mut header)?;

            if &header[0..16] != b"SQLite format 3\0" {
                bail!("Not a SQLite3 database (bad magic)");
            }

            let raw = u16::from_be_bytes([header[16], header[17]]);
            let page_size = if raw == 1 { 65_536usize } else { raw as usize };

            let mut page = vec![0u8; page_size];
            page[..100].copy_from_slice(&header);
            if page_size > 100 {
                file.read_exact(&mut page[100..])?;
            }

            let page_header_off = 100usize;
            let cell_count =
                u16::from_be_bytes([page[page_header_off + 3], page[page_header_off + 4]]) as usize;
            let pointer_base = page_header_off + 8;
            let mut output = String::new();

            for i in 0..cell_count {
                let off_idx = pointer_base + i * 2;
                let cell_off = u16::from_be_bytes([page[off_idx], page[off_idx + 1]]) as usize;
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
        _ => bail!("Missing or invalid command passed: {}", command),
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
