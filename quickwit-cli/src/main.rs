/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use anyhow::{bail, Context};
use byte_unit::Byte;
use clap::{load_yaml, value_t, App, AppSettings, ArgMatches};
use quickwit_cli::*;
use quickwit_common::to_socket_addr;
use quickwit_serve::serve_cli;
use quickwit_serve::ServeArgs;
use quickwit_telemetry::payload::TelemetryEvent;
use std::env;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::fmt::Subscriber;
#[derive(Debug, PartialEq)]
enum CliCommand {
    New(CreateIndexArgs),
    Index(IndexDataArgs),
    Search(SearchIndexArgs),
    Serve(ServeArgs),
    GarbageCollect(GarbageCollectIndexArgs),
    Delete(DeleteIndexArgs),
}
impl CliCommand {
    fn default_log_level(&self) -> Level {
        match self {
            CliCommand::New(_) => Level::WARN,
            CliCommand::Index(_) => Level::WARN,
            CliCommand::Search(_) => Level::WARN,
            CliCommand::Serve(_) => Level::INFO,
            CliCommand::GarbageCollect(_) => Level::WARN,
            CliCommand::Delete(_) => Level::WARN,
        }
    }

    fn parse_cli_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches_opt) = matches.subcommand();
        let submatches =
            submatches_opt.ok_or_else(|| anyhow::anyhow!("Unable to parse sub matches"))?;

        match subcommand {
            "new" => Self::parse_new_args(submatches),
            "index" => Self::parse_index_args(submatches),
            "search" => Self::parse_search_args(submatches),
            "serve" => Self::parse_serve_args(submatches),
            "gc" => Self::parse_garbage_collect_args(submatches),
            "delete" => Self::parse_delete_args(submatches),
            _ => bail!("Subcommand '{}' is not implemented", subcommand),
        }
    }

    fn parse_new_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let index_config_path = matches
            .value_of("index-config-path")
            .map(PathBuf::from)
            .context("'index-config-path' is a required arg")?;
        let overwrite = matches.is_present("overwrite");
        Ok(CliCommand::New(CreateIndexArgs::new(
            index_uri,
            index_config_path,
            overwrite,
        )?))
    }

    fn parse_index_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("index-uri is a required arg")?
            .to_string();
        let input_path: Option<PathBuf> = matches.value_of("input-path").map(PathBuf::from);
        let temp_dir: Option<PathBuf> = matches.value_of("temp-dir").map(PathBuf::from);
        let num_threads = value_t!(matches, "num-threads", usize)?; // 'num-threads' has a default value
        let heap_size_str = matches
            .value_of("heap-size")
            .context("heap-size has a default value")?;
        let heap_size = Byte::from_str(heap_size_str)?.get_bytes() as u64;
        let overwrite = matches.is_present("overwrite");
        Ok(CliCommand::Index(IndexDataArgs {
            index_uri,
            input_path,
            temp_dir,
            num_threads,
            heap_size,
            overwrite,
        }))
    }

    fn parse_search_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let query = matches
            .value_of("query")
            .context("query is a required arg")?
            .to_string();
        let max_hits = value_t!(matches, "max-hits", usize)?;
        let start_offset = value_t!(matches, "start-offset", usize)?;
        let search_fields = matches
            .values_of("search-fields")
            .map(|values| values.map(|value| value.to_string()).collect());
        let start_timestamp = if matches.is_present("start-timestamp") {
            Some(value_t!(matches, "start-timestamp", i64)?)
        } else {
            None
        };
        let end_timestamp = if matches.is_present("end-timestamp") {
            Some(value_t!(matches, "end-timestamp", i64)?)
        } else {
            None
        };

        Ok(CliCommand::Search(SearchIndexArgs {
            index_uri,
            query,
            max_hits,
            start_offset,
            search_fields,
            start_timestamp,
            end_timestamp,
        }))
    }

    fn parse_serve_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uris = matches
            .values_of("index-uri")
            .map(|values| {
                values
                    .into_iter()
                    .map(|index_uri| index_uri.to_string())
                    .collect()
            })
            .context("At least one 'index-uri' is required.")?;
        let host = matches
            .value_of("host")
            .context("'host' has a default value")?
            .to_string();
        let port = value_t!(matches, "port", u16)?;
        let rest_addr = format!("{}:{}", host, port);
        let rest_socket_addr = to_socket_addr(&rest_addr)?;

        let host_key_path_prefix = matches
            .value_of("host-key-path-prefix")
            .context("'host-key-path-prefix' has a default  value")?
            .to_string();

        let host_key_path =
            Path::new(format!("{}-{}-{}", host_key_path_prefix, host, port.to_string()).as_str())
                .to_path_buf();
        let mut peer_socket_addrs: Vec<SocketAddr> = Vec::new();
        if matches.is_present("peer-seed") {
            if let Some(values) = matches.values_of("peer-seed") {
                for value in values {
                    peer_socket_addrs.push(to_socket_addr(value)?);
                }
            }
        }

        Ok(CliCommand::Serve(ServeArgs {
            index_uris,
            rest_socket_addr,
            host_key_path,
            peer_socket_addrs,
        }))
    }
    fn parse_delete_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let dry_run = matches.is_present("dry-run");
        Ok(CliCommand::Delete(DeleteIndexArgs { index_uri, dry_run }))
    }
    fn parse_garbage_collect_args(matches: &ArgMatches) -> anyhow::Result<Self> {
        let index_uri = matches
            .value_of("index-uri")
            .context("'index-uri' is a required arg")?
            .to_string();
        let dry_run = matches.is_present("dry-run");
        Ok(CliCommand::GarbageCollect(GarbageCollectIndexArgs {
            index_uri,
            dry_run,
        }))
    }
}

fn setup_logger(default_level: Level) {
    if env::var("RUST_LOG").is_ok() {
        tracing_subscriber::fmt::init();
        return;
    }
    Subscriber::builder()
        .with_env_filter(format!("quickwit={}", default_level))
        .try_init()
        .expect("Failed to set up logger.")
}

#[tracing::instrument]
#[tokio::main]
async fn main() {
    let telemetry_handle = quickwit_telemetry::start_telemetry_loop();
    let about_text = about_text();

    let yaml = load_yaml!("cli.yaml");
    let app = App::from(yaml)
        .setting(AppSettings::ArgRequiredElseHelp)
        .version(env!("CARGO_PKG_VERSION"))
        .about(about_text.as_str());
    let matches = app.get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {:?}", err);
            std::process::exit(1);
        }
    };

    setup_logger(command.default_log_level());

    let command_res = match command {
        CliCommand::New(args) => create_index_cli(args).await,
        CliCommand::Index(args) => index_data_cli(args).await,
        CliCommand::Search(args) => search_index_cli(args).await,
        CliCommand::Serve(args) => serve_cli(args).await,
        CliCommand::GarbageCollect(args) => garbage_collect_index_cli(args).await,
        CliCommand::Delete(args) => delete_index_cli(args).await,
    };

    let return_code: i32 = if let Err(err) = command_res {
        eprintln!("Command failed: {:?}", err);
        1
    } else {
        0
    };

    quickwit_telemetry::send_telemetry_event(TelemetryEvent::EndCommand { return_code }).await;

    telemetry_handle.terminate_telemetry().await;

    std::process::exit(return_code);
}

/// Return the about text with telemetry info.
fn about_text() -> String {
    let mut about_text = format!("Indexing your large dataset on object storage & making it searchable from the command line.\nCommit hash: {}\n", env!("GIT_COMMIT_HASH"));
    if quickwit_telemetry::is_telemetry_enabled() {
        about_text += "Telemetry Enabled";
    }
    about_text
}

#[cfg(test)]
mod tests {
    use crate::{
        CliCommand, CreateIndexArgs, DeleteIndexArgs, GarbageCollectIndexArgs, IndexDataArgs,
        SearchIndexArgs,
    };
    use clap::{load_yaml, App, AppSettings};
    use quickwit_common::to_socket_addr;
    use quickwit_serve::ServeArgs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_new_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches_result =
            app.get_matches_from_safe(vec!["new", "--index-uri", "file:///indexes/wikipedia"]);
        assert!(matches!(matches_result, Err(_)));
        let mut index_config_file = NamedTempFile::new()?;
        let index_config_str = r#"{
            "type": "default",
            "store_source": true,
            "default_search_fields": ["timestamp"],
            "timestamp_field": "timestamp",
            "field_mappings": [
                {
                    "name": "timestamp",
                    "type": "i64",
                    "fast": true
                }
            ]
        }"#;
        index_config_file.write_all(index_config_str.as_bytes())?;
        let path = index_config_file.into_temp_path();
        let path_str = path.to_string_lossy().to_string();
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--index-config-path",
            &path_str,
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        let expected_cmd = CliCommand::New(
            CreateIndexArgs::new(
                "file:///indexes/wikipedia".to_string(),
                path.to_path_buf(),
                false,
            )
            .unwrap(),
        );
        assert_eq!(command.unwrap(), expected_cmd);

        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "new",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--index-config-path",
            &path_str,
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        let expected_cmd = CliCommand::New(
            CreateIndexArgs::new(
                "file:///indexes/wikipedia".to_string(),
                path.to_path_buf(),
                true,
            )
            .unwrap(),
        );
        assert_eq!(command.unwrap(), expected_cmd);

        Ok(())
    }

    #[test]
    fn test_parse_index_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["index", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_path: None,
                temp_dir: None,
                num_threads: 2,
                heap_size: 2_000_000_000,
                overwrite: false,
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "index",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--input-path",
            "/data/wikipedia.json",
            "--temp-dir",
            "./tmp",
            "--num-threads",
            "4",
            "--heap-size",
            "4gib",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Index(IndexDataArgs {
                index_uri,
                input_path: Some(input_path),
                temp_dir,
                num_threads: 4,
                heap_size: 4_294_967_296,
                overwrite: true,
            })) if &index_uri == "file:///indexes/wikipedia" && input_path == Path::new("/data/wikipedia.json") && temp_dir == Some(PathBuf::from("./tmp"))
        ));

        Ok(())
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-uri",
            "./wikipedia",
            "--query",
            "Barack Obama",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
                index_uri,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                start_timestamp: None,
                end_timestamp: None,
            })) if index_uri == "./wikipedia" && query == "Barack Obama"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "search",
            "--index-uri",
            "./wikipedia",
            "--query",
            "Barack Obama",
            "--max-hits",
            "50",
            "--start-offset",
            "100",
            "--search-fields",
            "title",
            "url",
            "--start-timestamp",
            "0",
            "--end-timestamp",
            "1",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Search(SearchIndexArgs {
                index_uri,
                query,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
            })) if index_uri == "./wikipedia" && query == "Barack Obama" && field_names == vec!["title".to_string(), "url".to_string()]
        ));

        Ok(())
    }

    #[test]
    fn test_parse_delete_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["delete", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: false
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "delete",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Delete(DeleteIndexArgs {
                index_uri,
                dry_run: true
            })) if &index_uri == "file:///indexes/wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_garbage_collect_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches =
            app.get_matches_from_safe(vec!["gc", "--index-uri", "file:///indexes/wikipedia"])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_uri,
                dry_run: false
            })) if &index_uri == "file:///indexes/wikipedia"
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "gc",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_uri,
                dry_run: true
            })) if &index_uri == "file:///indexes/wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_serve_args() -> anyhow::Result<()> {
        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "serve",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
            "--host-key-path-prefix",
            "/etc/quickwit-host-key",
            "--peer-seed",
            "192.168.1.13:9090",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Serve(ServeArgs {
                index_uris, rest_socket_addr, host_key_path, peer_socket_addrs
            })) if index_uris == vec!["file:///indexes/wikipedia".to_string()] && rest_socket_addr == to_socket_addr("127.0.0.1:9090").unwrap() && host_key_path == Path::new("/etc/quickwit-host-key-127.0.0.1-9090").to_path_buf() && peer_socket_addrs == vec![to_socket_addr("192.168.1.13:9090").unwrap()]
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "serve",
            "--index-uri",
            "file:///indexes/wikipedia",
            "--index-uri",
            "file:///indexes/hdfslogs",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
            "--host-key-path-prefix",
            "/etc/quickwit-host-key",
            "--peer-seed",
            "192.168.1.13:9090",
            "--peer-seed",
            "192.168.1.14:9090",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Serve(ServeArgs {
                index_uris, rest_socket_addr, host_key_path, peer_socket_addrs
            })) if index_uris == vec!["file:///indexes/wikipedia".to_string(), "file:///indexes/hdfslogs".to_string()] && rest_socket_addr == to_socket_addr("127.0.0.1:9090").unwrap() && host_key_path == Path::new("/etc/quickwit-host-key-127.0.0.1-9090").to_path_buf() && peer_socket_addrs == vec![to_socket_addr("192.168.1.13:9090").unwrap(), to_socket_addr("192.168.1.14:9090").unwrap()]
        ));

        let yaml = load_yaml!("cli.yaml");
        let app = App::from(yaml).setting(AppSettings::NoBinaryName);
        let matches = app.get_matches_from_safe(vec![
            "serve",
            "--index-uri",
            "file:///indexes/wikipedia,file:///indexes/hdfslogs",
            "--host",
            "127.0.0.1",
            "--port",
            "9090",
            "--host-key-path-prefix",
            "/etc/quickwit-host-key",
            "--peer-seed",
            "192.168.1.13:9090,192.168.1.14:9090",
        ])?;
        let command = CliCommand::parse_cli_args(&matches);
        assert!(matches!(
            command,
            Ok(CliCommand::Serve(ServeArgs {
                index_uris, rest_socket_addr, host_key_path, peer_socket_addrs
            })) if index_uris == vec!["file:///indexes/wikipedia".to_string(), "file:///indexes/hdfslogs".to_string()] && rest_socket_addr == to_socket_addr("127.0.0.1:9090").unwrap() && host_key_path == Path::new("/etc/quickwit-host-key-127.0.0.1-9090").to_path_buf() && peer_socket_addrs == vec![to_socket_addr("192.168.1.13:9090").unwrap(), to_socket_addr("192.168.1.14:9090").unwrap()]
        ));

        Ok(())
    }
}
