use anyhow::{anyhow, bail, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::types::Command;

pub async fn parse_command_from_stream<R>(reader: &mut R) -> Result<Option<Command>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    if !line.starts_with('*') {
        bail!("Invalid command format: expected array specifier ('*')");
    }
    let num_args: usize = line[1..].trim().parse()?;
    let mut args = Vec::with_capacity(num_args);
    for _ in 0..num_args {
        line.clear();
        reader.read_line(&mut line).await?;
        if !line.starts_with('$') {
            bail!("Invalid command format: expected bulk string specifier ('$')");
        }
        let len: usize = line[1..].trim().parse()?;
        let mut arg_buf = vec![0; len];
        reader.read_exact(&mut arg_buf).await?;
        args.push(arg_buf);
        line.clear();
        reader.read_line(&mut line).await?; // Read trailing CRLF
    }
    let name = String::from_utf8(
        args.get(0)
            .ok_or_else(|| anyhow!("Command name not provided"))?
            .clone(),
    )?
    .to_uppercase();
    Ok(Some(Command { name, args }))
}









