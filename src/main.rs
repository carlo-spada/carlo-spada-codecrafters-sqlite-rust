use anyhow::{bail, Result};
use std::fs::File;

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

    // Magic check (already present in your template, add if missing)
    if &header[0..16] != b"SQLite format 3\0" {
        anyhow::bail!("Not a SQLite3 database (bad magic)");
    }

    // Page size in bytes 16..18 (big-endian); raw=1 means 65536
    let raw = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if raw == 1 { 65536 } else { raw as u32 };

    println!("database page size: {}", page_size);

    // --- NEW: read page header of page 1 and count cells ---
    use std::io::{Read, Seek, SeekFrom};
    file.seek(SeekFrom::Start(100))?;
    let mut page_header = [0u8; 8];
    file.read_exact(&mut page_header)?;
    let cell_count = u16::from_be_bytes([page_header[3], page_header[4]]);
    println!("number of tables: {}", cell_count);
}
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
